# SPDX-FileCopyrightText: 2025 The python_openobserve authors
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gcr.io/oss-fuzz-base/base-builder-python@sha256:eeb64afa4f9a9d428e4f36b99687e29017981d841b17e7727cef2ee652b0acc8
COPY . $SRC/
WORKDIR $SRC
COPY .clusterfuzzlite/build.sh $SRC/
