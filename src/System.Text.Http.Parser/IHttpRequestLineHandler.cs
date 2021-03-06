// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace System.Text.Http.Parser
{
    public interface IHttpRequestLineHandler
    {
        void OnStartLine(Http.Method method, Http.Version version, Span<byte> target, Span<byte> path, Span<byte> query, Span<byte> customMethod, bool pathEncoded);
    }
}