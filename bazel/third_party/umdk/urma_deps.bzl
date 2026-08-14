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

"""Conditional fetching of UMDK (URMA) headers for both WORKSPACE and
MODULE.bazel.

Mirrors the CMake DOWNLOAD_URMA_HEADERS option: by default the UMDK headers
are downloaded so that --define BRPC_WITH_URMA=true works out of the box.
Set BRPC_DOWNLOAD_URMA_HEADERS=0 (via --repo_env) to skip the download when
the UMDK headers are already available on the system.

## MODULE.bazel usage

    urma = use_extension(
        "//bazel/third_party/umdk:urma_deps.bzl",
        "urma_deps",
    )
    urma.urma(download_headers = True)
    use_repo(urma, "umdk")

## WORKSPACE usage

    load("//bazel/third_party/umdk:urma_deps.bzl", "maybe_fetch_umdk")
    maybe_fetch_umdk()

    By default the UMDK repo is fetched.  To skip (e.g. when headers are
    installed system-wide), build with:
      --repo_env=BRPC_DOWNLOAD_URMA_HEADERS=0

## Command line

    bazel build //example:urma_performance_server --define BRPC_WITH_URMA=true
"""

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

_UMDK_COMMIT = "564ee727a55523d4351a8fb3c94292b388ebb924"  # v26.06.0_CAM
_UMDK_REMOTE = "https://atomgit.com/openeuler/umdk.git"

# Build file content for the UMDK repository, inlined so that the
# repository_rule can write it without needing a separate file.
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

# ---------------------------------------------------------------------------
# WORKSPACE helper
# ---------------------------------------------------------------------------

def _umdk_repo_impl(repository_ctx):
    """Fetches UMDK unless BRPC_DOWNLOAD_URMA_HEADERS=0.

    Defaults to fetching (mirrors CMake DOWNLOAD_URMA_HEADERS=ON).  When the
    environment variable is set to "0", an empty repository is created instead
    so that builds with system-installed UMDK headers are not blocked on a
    git clone.
    """
    if repository_ctx.os.environ.get("BRPC_DOWNLOAD_URMA_HEADERS", "1") != "0":
        repository_ctx.download_and_extract(
            url = _UMDK_REMOTE + "/repository/archive/" + _UMDK_COMMIT + ".tar.gz",
            stripPrefix = "umdk-" + _UMDK_COMMIT,
        )
        repository_ctx.file("BUILD.bazel", content = _UMDK_BUILD_CONTENT)
    else:
        # Create an empty repository so that Bazel does not error on the
        # @umdk workspace declaration.  Any target that references
        # @umdk//:urma_headers must be guarded by a select() on
        # //bazel/config:brpc_with_urma, which is the case in the brpc
        # BUILD files.
        repository_ctx.file("BUILD.bazel", content = """\
# Empty repository.  UMDK headers are not downloaded because
# BRPC_DOWNLOAD_URMA_HEADERS=0.  The system-installed headers
# are expected to be found via the select() in //:brpc.
""")

_umdk_repo = repository_rule(
    implementation = _umdk_repo_impl,
    environ = ["BRPC_DOWNLOAD_URMA_HEADERS"],
)

def maybe_fetch_umdk():
    """Fetch @umdk by default; skip when BRPC_DOWNLOAD_URMA_HEADERS=0.

    Intended for WORKSPACE files.  The default behavior downloads the UMDK
    headers so that --define BRPC_WITH_URMA=true works without extra flags.
    """
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

urma = tag_class(
    attrs = {
        "download_headers": attr.bool(
            default = True,
            doc = "Download UMDK headers from the upstream git repository. " +
                  "Set to False if the headers are already available on the " +
                  "system or if URMA support is not needed.",
        ),
    },
)

urma_deps = module_extension(
    implementation = _urma_impl,
    tag_classes = {"urma": urma},
)
