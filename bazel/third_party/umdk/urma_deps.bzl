# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""URMA (UMDK) header dependency for WORKSPACE and MODULE.bazel.

Mirrors the CMake DOWNLOAD_URMA_HEADERS option.

## MODULE.bazel

    urma = use_extension("//bazel/third_party/umdk:urma_deps.bzl", "urma_deps")
    urma.urma(download_headers = True)
    use_repo(urma, "umdk")

## WORKSPACE

    load("//bazel/third_party/umdk:urma_deps.bzl", "maybe_fetch_umdk")
    maybe_fetch_umdk()

## Command line

    bazel build //example:urma_performance_server --define BRPC_WITH_URMA=true
"""

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

_UMDK_COMMIT = "564ee727a55523d4351a8fb3c94292b388ebb924"  # v26.06.0_CAM
_UMDK_REMOTE = "https://atomgit.com/openeuler/umdk.git"

_UMDK_BUILD_CONTENT = """\
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma_headers",
    hdrs = glob([
        "src/urma/lib/urma/bond/include/*.h",
        "src/urma/lib/urma/core/include/*.h",
    ]),
    includes = [
        "src/urma/lib/urma/bond/include",
        "src/urma/lib/urma/core/include",
    ],
)
"""

_UMDK_STUB_BUILD_CONTENT = """\
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma_headers",
    hdrs = [],
    includes = [],
)
"""

# ---------------------------------------------------------------------------
# Shared stub repo rule (used by both WORKSPACE and MODULE.bazel paths)
# ---------------------------------------------------------------------------

def _umdk_stub_impl(repository_ctx):
    """Creates a minimal @umdk repo with an empty urma_headers target."""
    repository_ctx.file("BUILD.bazel", content = _UMDK_STUB_BUILD_CONTENT)

_umdk_stub = repository_rule(
    implementation = _umdk_stub_impl,
)

# ---------------------------------------------------------------------------
# WORKSPACE helper
# ---------------------------------------------------------------------------

def _umdk_repo_impl(repository_ctx):
    """Fetches UMDK unless BRPC_DOWNLOAD_URMA_HEADERS=0."""
    if repository_ctx.os.environ.get("BRPC_DOWNLOAD_URMA_HEADERS", "1") != "0":
        repository_ctx.download_and_extract(
            url = _UMDK_REMOTE + "/repository/archive/" + _UMDK_COMMIT + ".tar.gz",
            stripPrefix = "umdk-" + _UMDK_COMMIT,
        )
        repository_ctx.file("BUILD.bazel", content = _UMDK_BUILD_CONTENT)
    else:
        repository_ctx.file("BUILD.bazel", content = _UMDK_STUB_BUILD_CONTENT)

_umdk_repo = repository_rule(
    implementation = _umdk_repo_impl,
    environ = ["BRPC_DOWNLOAD_URMA_HEADERS"],
)

def maybe_fetch_umdk():
    """Fetch @umdk by default; stub when BRPC_DOWNLOAD_URMA_HEADERS=0."""
    _umdk_repo(name = "umdk")

# ---------------------------------------------------------------------------
# MODULE.bazel extension
# ---------------------------------------------------------------------------

def _urma_impl(ctx):
    """Implementation of the urma_deps module extension."""
    for mod in ctx.modules:
        for urma in mod.tags.urma:
            if urma.download_headers:
                git_repository(
                    name = "umdk",
                    build_file = "//bazel/third_party/umdk:umdk.BUILD",
                    remote = _UMDK_REMOTE,
                    commit = _UMDK_COMMIT,
                )
            else:
                _umdk_stub(name = "umdk")

urma = tag_class(
    attrs = {
        "download_headers": attr.bool(
            default = True,
            doc = "Download UMDK headers. Set to False to use a stub (no URMA).",
        ),
    },
)

urma_deps = module_extension(
    implementation = _urma_impl,
    tag_classes = {"urma": urma},
)
