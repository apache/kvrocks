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
	"net"
	"strings"
)

type tcpClient struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func newTCPClient(c net.Conn) *tcpClient {
	return &tcpClient{
		c: c,
		r: bufio.NewReader(c),
		w: bufio.NewWriter(c),
	}
}

func (c *tcpClient) Close() error {
	return c.c.Close()
}

func (c *tcpClient) ReadLine() (string, error) {
	r, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(r, "\r\n"), nil
}

func (c *tcpClient) Write(s string) error {
	_, err := c.w.WriteString(s)
	if err != nil {
		return err
	}
	return c.w.Flush()
}
