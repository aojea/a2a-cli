// Copyright 2026 The A2A Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/a2aproject/a2a-cli/internal/clierr"
	"github.com/a2aproject/a2a-cli/internal/controlapi"
)

func newServerMeshTokenCmd(cfg *globalConfig, sf *serverFlags) *cobra.Command {
	var (
		resources []string
		scope     string
		ttl       time.Duration
	)
	cmd := &cobra.Command{
		Use:   "mesh-token",
		Short: "Mint a sealed task-attenuated Agent Mesh Biscuit from this agent's in-process credential",
		Example: `  a2a server mesh-token --resource a2a://auditor
  a2a server mesh-token --resource a2a://auditor --ttl 15m`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(resources) == 0 && scope == "" {
				return clierr.Usage("pass at least one --resource (such as --resource a2a://auditor) or --scope")
			}
			c, err := sf.control()
			if err != nil {
				return err
			}
			resp, err := c.MeshToken(cmd.Context(), controlapi.MeshTokenRequest{
				Resources: resources,
				Scope:     scope,
				TTL:       ttl,
			})
			if err != nil {
				return err
			}
			if cfg.IsJSON() {
				return cfg.PrintJSON(resp)
			}
			_, err = fmt.Fprintln(cfg.Out, resp.Token)
			return err
		},
	}
	f := cmd.Flags()
	f.StringArrayVar(&resources, "resource", nil, "Target mesh service URI to scope the Biscuit to, e.g. a2a://auditor (repeatable)")
	f.StringVar(&scope, "scope", "", "Optional space-delimited operation scopes (e.g. \"method:POST\")")
	f.DurationVar(&ttl, "ttl", 0, "Optional maximum lifetime for the attenuated Biscuit (capped at member credential expiry)")
	return cmd
}
