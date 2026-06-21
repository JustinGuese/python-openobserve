# SPDX-FileCopyrightText: 2025 The python_openobserve authors
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gcr.io/oss-fuzz-base/base-builder-python@sha256:53a670f691b0bd348aa9f97a512918611ed960132c0fc124a12c2bc49ddec20a
COPY . $SRC/
WORKDIR $SRC
COPY .clusterfuzzlite/build.sh $SRC/
