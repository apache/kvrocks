//go:build compat

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package compatibility

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	dimage "github.com/docker/docker/api/types/image"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	kvrocksPort    = "6666"
	kvrocksDataDir = "/var/lib/kvrocks/db"
	defaultVersion = "v2.3.0"
)

var oldVersion = os.Getenv("KVROCKS_OLD_VERSION")
var newVersion = os.Getenv("KVROCKS_NEW_VERSION")

func init() {
	if oldVersion == "" {
		oldVersion = defaultVersion
	}
	if newVersion == "" {
		newVersion = "latest"
	}
}

func startKvrocksContainer(ctx context.Context, t *testing.T, image, volumeName string) (testcontainers.Container, error) {
	client, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect to docker: %w", err)
	}
	imageInspect, err := client.ImageInspect(ctx, image)
	if err != nil {
		// Image not present locally (e.g. fresh CI runner): pull it first.
		// Container creation would pull anyway, but the entrypoint rewrite
		// below needs the image's config.
		rc, pullErr := client.ImagePull(ctx, image, dimage.PullOptions{})
		if pullErr != nil {
			return nil, fmt.Errorf("pull image %s: %w", image, pullErr)
		}
		_, _ = io.Copy(io.Discard, rc) // consume until the pull completes
		rc.Close()
		imageInspect, err = client.ImageInspect(ctx, image)
		if err != nil {
			return nil, fmt.Errorf("inspect image %s after pull: %w", image, err)
		}
	}
	originalEntrypointCmd := strings.Join(imageInspect.Config.Entrypoint, " ")
	workingDir := imageInspect.Config.WorkingDir

	// older images had default user as root; in the entrypoint, we check if we're running as uid 999, then:
	// 1. set ownership of the database mounted volume, as older versions will have this directory owned by root,
	// 2. change user to 999:999 kvrocks
	preambleCmd := fmt.Sprintf("[ $(id -u) -eq 999 ] && chown -R 999:999 /var/lib/kvrocks/db && su kvrocks || cd %s", workingDir)
	// Use /bin/sh: alpine-based images (v2.6.0, v2.7.0) have no bash. Bind to
	// 0.0.0.0: older entrypoints omit --bind and their conf defaults to
	// 127.0.0.1, unreachable via the published port.
	entrypoint := []string{"/bin/sh", "-c", preambleCmd + " && " + originalEntrypointCmd + " --bind 0.0.0.0"}

	req := testcontainers.ContainerRequest{
		Image:        image,
		Entrypoint:   entrypoint,
		ExposedPorts: []string{fmt.Sprintf("%s/tcp", kvrocksPort)},
		User:         "root",
		Mounts: []testcontainers.ContainerMount{
			{
				Source: testcontainers.GenericVolumeMountSource{Name: volumeName},
				Target: kvrocksDataDir,
			},
		},
		WaitingFor: wait.ForListeningPort(kvrocksPort),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func terminateContainer(ctx context.Context, c testcontainers.Container, volumeName string) {
	err := c.Terminate(ctx, testcontainers.RemoveVolumes(volumeName))
	if err != nil {
		fmt.Printf("Warning: failed to terminate container and remove volume: %v\n", err)
	}
}

func getContainerAddr(c testcontainers.Container, ctx context.Context, t *testing.T) string {
	hostPort, err := c.MappedPort(ctx, kvrocksPort)
	require.NoError(t, err)
	// Host() returns "localhost", which can resolve to ::1 and fail on hosts
	// without IPv6 docker bindings; dial the IPv4 loopback directly.
	return fmt.Sprintf("127.0.0.1:%s", hostPort.Port())
}

func TestCompatibilityAllTypes(t *testing.T) {
	ctx := context.Background()

	// Unique per run: a crashed container leaks its volume, and a reused name
	// would then make every later run fail opening RocksDB (LOCK contention).
	volumeName := fmt.Sprintf("kvrocks-compat-%s-%s-%d", oldVersion, t.Name(), time.Now().UnixNano())
	// Docker Hub tags releases without the 'v' prefix (e.g. 2.3.0, not v2.3.0)
	oldImage := fmt.Sprintf("docker.io/apache/kvrocks:%s", strings.TrimPrefix(oldVersion, "v"))
	// newImage is env-overridable so CI can build the PR's own source and test
	// against it instead of the last released image.
	newImage := fmt.Sprintf("docker.io/apache/kvrocks:%s", newVersion)

	// Start old kvrocks and populate data
	oldC, err := startKvrocksContainer(ctx, t, oldImage, volumeName)
	require.NoError(t, err)

	oldClient := redis.NewClient(&redis.Options{Addr: getContainerAddr(oldC, ctx, t)})
	require.NoError(t, oldClient.Ping(ctx).Err())

	// Populate test data based on old version capabilities
	testData, err := PopulateTestData(ctx, oldClient, oldVersion)
	require.NoError(t, err)

	require.NoError(t, oldClient.Close())
	require.NoError(t, oldC.Terminate(ctx))

	// Start new kvrocks with same volume and verify data
	newC, err := startKvrocksContainer(ctx, t, newImage, volumeName)
	require.NoError(t, err)
	defer terminateContainer(ctx, newC, volumeName)

	newClient := redis.NewClient(&redis.Options{Addr: getContainerAddr(newC, ctx, t)})
	defer func() { require.NoError(t, newClient.Close()) }()

	require.NoError(t, newClient.Ping(ctx).Err())
	require.NoError(t, VerifyTestData(ctx, newClient, testData))
}
