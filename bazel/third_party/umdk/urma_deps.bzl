# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

_UMDK_GIT_URL = "https://atomgit.com/openeuler/umdk.git"
_UMDK_GIT_TAG = "main"

urma_tag = tag_class(attrs = {
    "download_headers": attr.bool(
        default = True,
        doc = "Download UMDK URMA headers from upstream. When False, an empty stub is created.",
    ),
})

def _urma_deps_impl(ctx):
    download_headers = True
    for mod in ctx.modules:
        for urma in mod.tags.urma:
            download_headers = urma.download_headers

    if download_headers:
        ctx.download_and_extract(
            url = "%s/archive/%s.tar.gz" % (_UMDK_GIT_URL, _UMDK_GIT_TAG),
            output = "umdk_src",
            stripPrefix = "umdk-%s/src/urma/lib/urma/core/include" % _UMDK_GIT_TAG,
        )
        ctx.file(
            "umdk/BUILD.bazel",
            content = """
package(default_visibility = ["//visibility:public"])
cc_library(
    name = "urma_headers",
    hdrs = glob(["**/*.h"]),
    includes = ["."],
)
""",
        )
        ctx.symlink("../../umdk_src", "umdk/include")
    else:
        ctx.file(
            "umdk/BUILD.bazel",
            content = """
package(default_visibility = ["//visibility:public"])
cc_library(
    name = "urma_headers",
    hdrs = [],
    includes = [],
)
""",
        )

urma_deps = module_extension(
    implementation = _urma_deps_impl,
    tag_classes = {"urma": urma_tag},
)
