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

# Build file content for the UMDK repository (downloaded case).
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

# Build file content for the stub repository (system-headers case).
# Provides the same "urma_headers" target so that select() references
# to @umdk//:urma_headers resolve, but uses system include directories
# instead of downloaded sources.  The include directories follow the
# same search paths as CMake's find_path(URMA_INCLUDE_PATH ...).
_UMDK_STUB_BUILD_CONTENT = """\
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma_headers",
    hdrs = [],
    includes = [
        "core",
        "bond",
    ],
)

# Symlink the system-installed UMDK header directories into the repo
# so that Bazel can see them as local includes.  The paths follow
# CMake's search order (ub/umdk/urma under /usr/include, /usr/local,
# and URMA_ROOT).
"""

# ---------------------------------------------------------------------------
# WORKSPACE helper
# ---------------------------------------------------------------------------

def _umdk_repo_impl(repository_ctx):
    """Fetches UMDK unless BRPC_DOWNLOAD_URMA_HEADERS=0.

    Defaults to fetching (mirrors CMake DOWNLOAD_URMA_HEADERS=ON).  When the
    environment variable is set to "0", a stub repository is created that
    exposes system-installed UMDK headers via symlinks, so that
    @umdk//:urma_headers resolves correctly.
    """
    if repository_ctx.os.environ.get("BRPC_DOWNLOAD_URMA_HEADERS", "1") != "0":
        repository_ctx.download_and_extract(
            url = _UMDK_REMOTE + "/repository/archive/" + _UMDK_COMMIT + ".tar.gz",
            stripPrefix = "umdk-" + _UMDK_COMMIT,
        )
        repository_ctx.file("BUILD.bazel", content = _UMDK_BUILD_CONTENT)
    else:
        # Create a stub repository that symlinks to system-installed UMDK
        # headers.  This mirrors CMake's find_path search: check URMA_ROOT
        # first, then /usr/local/include, then /usr/include.  The PATH_SUFFIXES
        # that CMake uses (ub/umdk/urma, umdk/urma) are resolved into the
        # final directory that contains urma_api.h.
        urma_root = repository_ctx.os.environ.get("URMA_ROOT", "")
        core_dir = ""
        bond_dir = ""

        search_roots = []
        if urma_root:
            search_roots.append(urma_root)
        search_roots.extend(["/usr/local/include", "/usr/include"])

        suffixes = ["ub/umdk/urma", "umdk/urma", "urma"]

        for root in search_roots:
            if core_dir:
                break
            for suffix in suffixes:
                candidate = root + "/" + suffix if suffix else root
                if repository_ctx.path(candidate).exists:
                    # Verify urma_api.h is present
                    if repository_ctx.path(candidate + "/urma_api.h").exists:
                        core_dir = candidate
                        # Bond headers live in the same directory
                        if repository_ctx.path(candidate + "/urma_ubagg.h").exists:
                            bond_dir = candidate
                        break
            # Also check the upstream source layout used by the git repo
            if not core_dir:
                for root2 in search_roots:
                    candidate = root2 + "/src/urma/lib/urma/core/include"
                    if repository_ctx.path(candidate + "/urma_api.h").exists:
                        core_dir = candidate
                        break
            if not bond_dir:
                for root2 in search_roots:
                    candidate = root2 + "/src/urma/lib/urma/bond/include"
                    if repository_ctx.path(candidate + "/urma_ubagg.h").exists:
                        bond_dir = candidate
                        break

        # Build the BUILD.bazel with the discovered system paths
        includes_list = []
        symlinks_cmds = []
        if core_dir:
            # Create a symlink so Bazel can treat the system headers as
            # part of the repository.
            repository_ctx.symlink(core_dir, "core")
            includes_list.append("core")
        if bond_dir:
            repository_ctx.symlink(bond_dir, "bond")
            includes_list.append("bond")

        includes_str = ",\n        ".join(
            ['"' + inc + '"' for inc in includes_list],
        )
        build_content = """\
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma_headers",
    hdrs = glob(["core/*.h", "bond/*.h"]),
    includes = [
        {includes}
    ],
)
""".format(includes = includes_str) if includes_list else """\
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma_headers",
    hdrs = [],
    includes = [],
)
"""
        repository_ctx.file("BUILD.bazel", content = build_content)

_umdk_repo = repository_rule(
    implementation = _umdk_repo_impl,
    environ = ["BRPC_DOWNLOAD_URMA_HEADERS", "URMA_ROOT"],
)

def maybe_fetch_umdk():
    """Fetch @umdk by default; skip when BRPC_DOWNLOAD_URMA_HEADERS=0.

    Intended for WORKSPACE files.  The default behavior downloads the UMDK
    headers so that --define BRPC_WITH_URMA=true works without extra flags.
    When BRPC_DOWNLOAD_URMA_HEADERS=0, system-installed UMDK headers are
    discovered via the same search paths as CMake.
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
