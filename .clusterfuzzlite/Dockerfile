# SPDX-FileCopyrightText: 2025 The python_openobserve authors
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gcr.io/oss-fuzz-base/base-builder-python@sha256:66c3a78c912a076695da349f2c0f21538be76bdfcba5d73603be45e39066cd38
COPY . $SRC/
WORKDIR $SRC
COPY .clusterfuzzlite/build.sh $SRC/
