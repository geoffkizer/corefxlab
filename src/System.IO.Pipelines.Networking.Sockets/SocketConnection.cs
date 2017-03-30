// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.IO.Pipelines.Networking.Sockets.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace System.IO.Pipelines.Networking.Sockets
{
    /// <summary>
    /// Represents an <see cref="IPipeConnection"/> implementation using the async Socket API
    /// </summary>
    public class SocketConnection : IPipeConnection
    {
        private static readonly EventHandler<SocketAsyncEventArgs> _asyncCompleted = OnAsyncCompleted;

        private static readonly Queue<SocketAsyncEventArgs> _argsPool = new Queue<SocketAsyncEventArgs>();

#if false
        private static readonly MicroBufferPool _smallBuffers;

        internal static int SmallBuffersInUse = _smallBuffers?.InUse ?? 0;

        const int SmallBufferSize = 8;
#endif

        // track the state of which strategy to use; need to use a known-safe
        // strategy until we can decide which to use (by observing behavior)
//        private static BufferStyle _bufferStyle;
//        private static bool _seenReceiveZeroWithAvailable, _seenReceiveZeroWithEOF;

        private static readonly byte[] _zeroLengthBuffer = new byte[0];


        private readonly bool _ownsFactory;
        private PipeFactory _factory;
        private IPipe _input, _output;
        private Socket _socket;
        private Task _receiveTask;
        private Task _sendTask;
        private volatile bool _stopping;
        private bool _disposed;

        static SocketConnection()
        {
#if false
            // validated styles for known OSes
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // zero-length receive works fine
                _bufferStyle = BufferStyle.UseZeroLengthBuffer;
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                || RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                // zero-length receive is unreliable
                _bufferStyle = BufferStyle.UseSmallBuffer;
            }
            else
            {
                // default to "figure it out based on what happens"
                _bufferStyle = BufferStyle.Unknown;
            }

            if (_bufferStyle != BufferStyle.UseZeroLengthBuffer)
            {
                // we're going to need to use small buffers for awaiting input
                _smallBuffers = new MicroBufferPool(SmallBufferSize, ushort.MaxValue);
            }
#endif
        }

        internal SocketConnection(Socket socket, PipeFactory factory)
        {
            socket.NoDelay = true;
            _socket = socket;
            if (factory == null)
            {
                _ownsFactory = true;
                factory = new PipeFactory();
            }
            _factory = factory;

#if false
            // TODO: Make this configurable
            // Dispatch to avoid deadlocks
            _input = PipeFactory.Create(new PipeOptions
            {
                ReaderScheduler = TaskRunScheduler.Default,
                WriterScheduler = TaskRunScheduler.Default
            });

            _output = PipeFactory.Create(new PipeOptions
            {
                ReaderScheduler = TaskRunScheduler.Default,
                WriterScheduler = TaskRunScheduler.Default
            });
#endif
            _input = PipeFactory.Create(new PipeOptions
            {
                ReaderScheduler = InlineScheduler.Default,
                WriterScheduler = InlineScheduler.Default
            });

            _output = PipeFactory.Create(new PipeOptions
            {
                ReaderScheduler = InlineScheduler.Default,
                WriterScheduler = InlineScheduler.Default
            });

            _receiveTask = ReceiveFromSocketAndPushToWriterAsync();
            _sendTask = ReadFromReaderAndWriteToSocketAsync();
        }

        /// <summary>
        /// Provides access to data received from the socket
        /// </summary>
        public IPipeReader Input => _input.Reader;

        /// <summary>
        /// Provides access to write data to the socket
        /// </summary>
        public IPipeWriter Output => _output.Writer;

        private PipeFactory PipeFactory => _factory;

        private Socket Socket => _socket;

        /// <summary>
        /// Begins an asynchronous connect operation to the designated endpoint
        /// </summary>
        /// <param name="endPoint">The endpoint to which to connect</param>
        /// <param name="factory">Optionally allows the underlying <see cref="PipeFactory"/> (and hence memory pool) to be specified; if one is not provided, a <see cref="PipeFactory"/> will be instantiated and owned by the connection</param>
        public static Task<SocketConnection> ConnectAsync(IPEndPoint endPoint, PipeFactory factory = null)
        {
            var args = new SocketAsyncEventArgs();
            args.RemoteEndPoint = endPoint;
            args.Completed += _asyncCompleted;
            var tcs = new TaskCompletionSource<SocketConnection>(factory);
            args.UserToken = tcs;
            if (!Socket.ConnectAsync(SocketType.Stream, ProtocolType.Tcp, args))
            {
                OnConnect(args); // completed sync - usually means failure
            }
            return tcs.Task;
        }
        /// <summary>
        /// Releases all resources owned by the connection
        /// </summary>
        public void Dispose() => Dispose(true);

        internal static SocketAsyncEventArgs GetOrCreateSocketAsyncEventArgs()
        {
            SocketAsyncEventArgs args = null;
            lock (_argsPool)
            {
                if (_argsPool.Count != 0)
                {
                    args = _argsPool.Dequeue();
                }
            }
            if (args == null)
            {
                args = new SocketAsyncEventArgs();
                args.Completed += _asyncCompleted; // only for new, otherwise multi-fire
            }
            if (args.UserToken is Signal)
            {
                ((Signal)args.UserToken).Reset();
            }
            else
            {
                args.UserToken = new Signal();
            }
            return args;
        }

        internal static void RecycleSocketAsyncEventArgs(SocketAsyncEventArgs args)
        {
            if (args != null)
            {
                args.SetBuffer(null, 0, 0); // make sure we don't keep a slab alive
                lock (_argsPool)
                {
                    if (_argsPool.Count < 2048)
                    {
                        _argsPool.Enqueue(args);
                    }
                }
            }
        }

        public async Task DisposeAsync()
        {
            if (!_disposed)
            {
                _disposed = true;
                _stopping = true;
                _output.Reader.CancelPendingRead();

                await Task.WhenAll(_sendTask, _receiveTask);

                _output.Writer.Complete();
                _input.Reader.Complete();

                _socket?.Dispose();
                _socket = null;
                if (_ownsFactory) { _factory?.Dispose(); }
                _factory = null;
            }
        }

        /// <summary>
        /// Releases all resources owned by the connection
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                GC.SuppressFinalize(this);

                DisposeAsync().GetAwaiter().GetResult();
            }
        }

        private static void OnAsyncCompleted(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                switch (e.LastOperation)
                {
                    case SocketAsyncOperation.Connect:
                        OnConnect(e);
                        break;

                    case SocketAsyncOperation.Send:
                    case SocketAsyncOperation.Receive:
                        ReleasePending(e);
                        break;
                }
            }
            catch { }
        }

        private static void OnConnect(SocketAsyncEventArgs e)
        {
            var tcs = (TaskCompletionSource<SocketConnection>)e.UserToken;
            try
            {
                if (e.SocketError == SocketError.Success)
                {
                    tcs.TrySetResult(new SocketConnection(e.ConnectSocket, (PipeFactory)tcs.Task.AsyncState));
                }
                else
                {
                    tcs.TrySetException(new SocketException((int)e.SocketError));
                }
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        }

        private static void ReleasePending(SocketAsyncEventArgs e)
        {
            var pending = (Signal)e.UserToken;
            pending.Set();
        }

        private enum BufferStyle
        {
            Unknown,
            UseZeroLengthBuffer,
            UseSmallBuffer
        }

        private async Task ReceiveFromSocketAndPushToWriterAsync()
        {
            SocketAsyncEventArgs args = null;
            try
            {
                // wait for someone to be interested in data before we
                // start allocating buffers and probing the socket
                args = GetOrCreateSocketAsyncEventArgs();
                while (!_stopping)
                {
                    // Ensure we have some reasonable amount of buffer space
                    WritableBuffer buffer = _input.Writer.Alloc(1024);

                    int len = 0;
                    try
                    {
                        SetBuffer(buffer.Buffer, args);

                        // await async for the io work to be completed
                        await Socket.ReceiveSignalAsync(args);
                        if (args.SocketError != SocketError.Success)
                        {
                            throw new SocketException((int)args.SocketError);
                        }

                        len = args.BytesTransferred;
                        if (len == 0)
                        {
                            // socket reported EOF
                            break;
                        }
                    }
                    finally
                    {
                        // record what data we filled into the buffer and push to pipe
                        buffer.Advance(len);
                        _stopping = (await buffer.FlushAsync()).IsCompleted;
                    }
                }

                _input.Writer.Complete();
            }
            catch (Exception ex)
            {
                // don't trust signal after an error; someone else could
                // still have it and invoke Set
                if (args != null)
                {
                    args.UserToken = null;
                }
                _input?.Writer.Complete(ex);
            }
            finally
            {
                try
                {
                    Socket.Shutdown(SocketShutdown.Receive);
                }
                catch { }

                RecycleSocketAsyncEventArgs(args);
            }
        }

#if false
        private static ArraySegment<byte> LeaseSmallBuffer()
        {
            ArraySegment<byte> result;
            if (!_smallBuffers.TryTake(out result))
            {
                // use a throw-away buffer as a fallback
                result = new ArraySegment<byte>(new byte[_smallBuffers.BytesPerItem]);
            }
            return result;
        }
        private void RecycleSmallBuffer(ref ArraySegment<byte> buffer)
        {
            if (buffer.Array != null)
            {
                _smallBuffers?.Recycle(buffer);
            }
            buffer = default(ArraySegment<byte>);
        }
#endif

        private async Task ReadFromReaderAndWriteToSocketAsync()
        {
            SocketAsyncEventArgs args = null;
            try
            {
                args = GetOrCreateSocketAsyncEventArgs();

                while (!_stopping)
                {
                    var result = await _output.Reader.ReadAsync();
                    var buffer = result.Buffer;
                    try
                    {
                        if (buffer.IsEmpty && result.IsCompleted)
                        {
                            break;
                        }

                        foreach (var memory in buffer)
                        {
                            int remaining = memory.Length;
                            while (remaining != 0)
                            {
                                SetBuffer(memory, args, memory.Length - remaining);

                                // await async for the io work to be completed
                                await Socket.SendSignalAsync(args);

                                // either way, need to validate
                                if (args.SocketError != SocketError.Success)
                                {
                                    throw new SocketException((int)args.SocketError);
                                }

                                remaining -= args.BytesTransferred;
                            }
                        }
                    }
                    finally
                    {
                        _output.Reader.Advance(buffer.End);
                    }
                }
                _output.Reader.Complete();
            }
            catch (Exception ex)
            {
                // don't trust signal after an error; someone else could
                // still have it and invoke Set
                if (args != null)
                {
                    args.UserToken = null;
                }
                _output?.Reader.Complete(ex);
            }
            finally
            {
                try // we're not going to be sending anything else
                {
                    Socket.Shutdown(SocketShutdown.Send);
                }
                catch { }
                RecycleSocketAsyncEventArgs(args);
            }
        }

        // unsafe+async not good friends
        private unsafe void SetBuffer(Buffer<byte> memory, SocketAsyncEventArgs args, int ignore = 0)
        {
            ArraySegment<byte> segment;
            if (!memory.TryGetArray(out segment))
            {
                throw new InvalidOperationException("Memory is not backed by an array; oops!");
            }
            args.SetBuffer(segment.Array, segment.Offset + ignore, segment.Count - ignore);
        }

        private void Shutdown()
        {
            Socket?.Shutdown(SocketShutdown.Both);
        }
    }
}