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

package util

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
)

type TcpClient struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func newTCPClient(c net.Conn) *TcpClient {
	return &TcpClient{
		c: c,
		r: bufio.NewReader(c),
		w: bufio.NewWriter(c),
	}
}

func (c *TcpClient) Close() error {
	return c.c.Close()
}

func (c *TcpClient) ReadLine() (string, error) {
	r, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(r, "\r\n"), nil
}

func (c *TcpClient) Write(s string) error {
	_, err := c.w.WriteString(s)
	if err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *TcpClient) WriteArgs(args ...string) error {
	if args == nil {
		return errors.New("args cannot be nil")
	}

	if len(args) == 0 {
		return errors.New("args cannot be empty")
	}

	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		cmd = cmd + fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	return c.Write(cmd)
}
