/*
 *	Firebird ADO.NET Data provider for .NET and Mono 
 * 
 *	   The contents of this file are subject to the Initial 
 *	   Developer's Public License Version 1.0 (the "License"); 
 *	   you may not use this file except in compliance with the 
 *	   License. You may obtain a copy of the License at 
 *	   http://www.firebirdsql.org/index.php?op=doc&id=idpl
 *
 *	   Software distributed under the License is distributed on 
 *	   an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either 
 *	   express or implied. See the License for the specific 
 *	   language governing rights and limitations under the License.
 * 
 *	Copyright (c) 2002, 2007 Carlos Guzman Alvarez
 *	All Rights Reserved.
 *   
 *  Contributors:
 *    Jiri Cincura (jiri@cincura.net)
 */

using System;
using System.Collections;
using System.IO;
using System.Net;
using System.Text;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Managed.Version10
{
	internal class XdrStream : Stream
	{
		#region · Static Fields ·

		private static byte[] fill;
		private static byte[] pad;

		#endregion

		#region · Static Properties ·

		internal static byte[] Fill
		{
			get
			{
				if (fill == null)
				{
					fill = new byte[32767];
					for (int i = 0; i < fill.Length; i++)
					{
						fill[i] = 32;
					}
				}

				return fill;
			}
		}

		private static byte[] Pad
		{
			get
			{
				if (pad == null)
				{
					pad = new byte[] { 0, 0, 0, 0 };
				}

				return pad;
			}
		}

		#endregion

		#region · Fields ·

		private Charset charset;
		private Stream innerStream;
		private int operation;

		#endregion

		#region · Stream Properties ·

		public override bool CanWrite
		{
			get { return this.innerStream.CanWrite; }
		}

		public override bool CanRead
		{
			get { return this.innerStream.CanRead; }
		}

		public override bool CanSeek
		{
			get { return this.innerStream.CanSeek; }
		}

		public override long Position
		{
			get { return this.innerStream.Position; }
			set { this.innerStream.Position = value; }
		}

		public override long Length
		{
			get { return this.innerStream.Length; }
		}

		#endregion

		#region · Constructors ·

		public XdrStream()
			: this(Charset.DefaultCharset)
		{ }

		public XdrStream(Charset charset)
			: this(new MemoryStream(), charset)
		{ }

		public XdrStream(byte[] buffer, Charset charset)
			: this(new MemoryStream(buffer), charset)
		{ }

		public XdrStream(Stream innerStream, Charset charset)
			: base()
		{
			this.innerStream = innerStream;
			this.charset = charset;
			this.ResetOperation();

			GC.SuppressFinalize(innerStream);
		}

		#endregion

		#region · Stream methods ·

		public override void Close()
		{
			try
			{
				if (this.innerStream != null)
				{
					this.innerStream.Close();
				}
			}
			catch
			{
			}
			finally
			{
				this.charset = null;
				this.innerStream = null;
			}
		}

		public override void Flush()
		{
			this.CheckDisposed();

			this.innerStream.Flush();
		}
		public override Task FlushAsync(CancellationToken cancellationToken)
		{
			this.CheckDisposed();

			return this.innerStream.FlushAsync(cancellationToken);
		}

		public override void SetLength(long length)
		{
			this.CheckDisposed();

			this.innerStream.SetLength(length);
		}

		public override long Seek(long offset, System.IO.SeekOrigin loc)
		{
			this.CheckDisposed();

			return this.innerStream.Seek(offset, loc);
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			this.CheckDisposed();

			if (this.CanRead)
			{
				return this.innerStream.Read(buffer, offset, count);
			}

			throw new InvalidOperationException("Read operations are not allowed by this stream");
		}
		public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
		{
			this.CheckDisposed();

			if (this.CanRead)
			{
				return this.innerStream.ReadAsync(buffer, offset, count, cancellationToken);
			}

			throw new InvalidOperationException("Read operations are not allowed by this stream");
		}

		public override void WriteByte(byte value)
		{
			this.CheckDisposed();

			this.innerStream.WriteByte(value);
		}
		public Task WriteByteAsync(byte value, CancellationToken cancellationToken)
		{
			this.CheckDisposed();

			return this.innerStream.WriteByteAsync(value, cancellationToken);
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			this.CheckDisposed();

			if (this.CanWrite)
			{
				this.innerStream.Write(buffer, offset, count);
			}
			else
			{
				throw new InvalidOperationException("Write operations are not allowed by this stream");
			}
		}
		public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
		{
			this.CheckDisposed();

			if (this.CanWrite)
			{
				return this.innerStream.WriteAsync(buffer, offset, count, cancellationToken);
			}
			else
			{
				throw new InvalidOperationException("Write operations are not allowed by this stream");
			}
		}

		public byte[] ToArray()
		{
			this.CheckDisposed();

			if (this.innerStream is MemoryStream)
			{
				return ((MemoryStream)this.innerStream).ToArray();
			}

			throw new InvalidOperationException();
		}

		#endregion

		#region · Operation Identification Methods ·

		public virtual int ReadOperation()
		{
			int op = this.ValidOperationAvailable ? this.operation : this.ReadNextOperation();
			this.ResetOperation();
			return op;
		}

		public virtual int ReadNextOperation()
		{
			do
			{
				/* loop	as long	as we are receiving	dummy packets, just
				 * throwing	them away--note	that if	we are a server	we won't
				 * be receiving	them, but it is	better to check	for	them at
				 * this	level rather than try to catch them	in all places where
				 * this	routine	is called 
				 */
				this.operation = this.ReadInt32();
			} while (this.operation == IscCodes.op_dummy);

			return this.operation;
		}

		#endregion

		#region · XDR Read Methods ·

		public byte[] ReadBytes(int count)
		{
			byte[] buffer = new byte[count];

			if (count > 0)
			{
				int toRead = count;
				int currentlyRead = -1;
				while (toRead > 0 && currentlyRead != 0)
				{
					toRead -= (currentlyRead = this.Read(buffer, count - toRead, toRead));
				}
				if (toRead == count)
				{
					throw new IOException();
				}
			}

			return buffer;
		}
		public async Task<byte[]> ReadBytesAsync(int count, CancellationToken cancellationToken)
		{
			byte[] buffer = new byte[count];

			if (count > 0)
			{
				int toRead = count;
				int currentlyRead = -1;
				while (toRead > 0 && currentlyRead != 0)
				{
					toRead -= (currentlyRead = await this.ReadAsync(buffer, count - toRead, toRead, cancellationToken).ConfigureAwait(false));
				}
				if (toRead == count)
				{
					throw new IOException();
				}
			}

			return buffer;
		}

		public byte[] ReadOpaque(int length)
		{
			byte[] buffer = this.ReadBytes(length);

			int padLength = ((4 - length) & 3);
			if (padLength > 0)
			{
				this.Read(Pad, 0, padLength);
			}

			return buffer;
		}
		public async Task<byte[]> ReadOpaqueAsync(int length, CancellationToken cancellationToken)
		{
			byte[] buffer = await this.ReadBytesAsync(length, cancellationToken).ConfigureAwait(false);

			int padLength = ((4 - length) & 3);
			if (padLength > 0)
			{
				await this.ReadAsync(Pad, 0, padLength, cancellationToken).ConfigureAwait(false);
			}

			return buffer;
		}

		public byte[] ReadBuffer()
		{
			return this.ReadOpaque((ushort)this.ReadInt32());
		}
		public async Task<byte[]> ReadBufferAsync(CancellationToken cancellationToken)
		{
			return await this.ReadOpaqueAsync((ushort)await this.ReadInt32Async(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
		}

		public string ReadString()
		{
			return this.ReadString(this.charset);
		}
		public Task<string> ReadStringAsync(CancellationToken cancellationToken)
		{
			return this.ReadStringAsync(this.charset, cancellationToken);
		}

		public string ReadString(int length)
		{
			return this.ReadString(this.charset, length);
		}
		public Task<string> ReadStringAsync(int length, CancellationToken cancellationToken)
		{
			return this.ReadStringAsync(this.charset, length, cancellationToken);
		}

		public string ReadString(Charset charset)
		{
			return this.ReadString(charset, this.ReadInt32());
		}
		public async Task<string> ReadStringAsync(Charset charset, CancellationToken cancellationToken)
		{
			return await this.ReadStringAsync(charset, await this.ReadInt32Async(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
		}

		public string ReadString(Charset charset, int length)
		{
			byte[] buffer = this.ReadOpaque(length);

			return charset.GetString(buffer, 0, buffer.Length);
		}
		public async Task<string> ReadStringAsync(Charset charset, int length, CancellationToken cancellationToken)
		{
			byte[] buffer = await this.ReadOpaqueAsync(length, cancellationToken).ConfigureAwait(false);

			return charset.GetString(buffer, 0, buffer.Length);
		}

		public short ReadInt16()
		{
			return Convert.ToInt16(this.ReadInt32());
		}
		public async Task<short> ReadInt16Async(CancellationToken cancellationToken)
		{
			return Convert.ToInt16(await this.ReadInt32Async(cancellationToken).ConfigureAwait(false));
		}

		public int ReadInt32()
		{
			return IPAddress.HostToNetworkOrder(BitConverter.ToInt32(this.ReadBytes(4), 0));
		}
		public async Task<int> ReadInt32Async(CancellationToken cancellationToken)
		{
			return IPAddress.HostToNetworkOrder(BitConverter.ToInt32(await this.ReadBytesAsync(4, cancellationToken).ConfigureAwait(false), 0));
		}

		public long ReadInt64()
		{
			return IPAddress.HostToNetworkOrder(BitConverter.ToInt64(this.ReadBytes(8), 0));
		}
		public async Task<long> ReadInt64Async(CancellationToken cancellationToken)
		{
			return IPAddress.HostToNetworkOrder(BitConverter.ToInt64(await this.ReadBytesAsync(8, cancellationToken).ConfigureAwait(false), 0));
		}

		public Guid ReadGuid(int length)
		{
			return new Guid(this.ReadOpaque(length));
		}
		public async Task<Guid> ReadGuidAsync(int length, CancellationToken cancellationToken)
		{
			return new Guid(await this.ReadOpaqueAsync(length, cancellationToken).ConfigureAwait(false));
		}

		public float ReadSingle()
		{
			return BitConverter.ToSingle(BitConverter.GetBytes(this.ReadInt32()), 0);
		}
		public async Task<float> ReadSingleAsync(CancellationToken cancellationToken)
		{
			return BitConverter.ToSingle(BitConverter.GetBytes(await this.ReadInt32Async(cancellationToken).ConfigureAwait(false)), 0);
		}

		public double ReadDouble()
		{
			return BitConverter.ToDouble(BitConverter.GetBytes(this.ReadInt64()), 0);
		}
		public async Task<double> ReadDoubleAsync(CancellationToken cancellationToken)
		{
			return BitConverter.ToDouble(BitConverter.GetBytes(await this.ReadInt64Async(cancellationToken).ConfigureAwait(false)), 0);
		}

		public DateTime ReadDateTime()
		{
			DateTime date = this.ReadDate();
			TimeSpan time = this.ReadTime();

			return new DateTime(date.Year, date.Month, date.Day, time.Hours, time.Minutes, time.Seconds, time.Milliseconds);
		}
		public async Task<DateTime> ReadDateTimeAsync(CancellationToken cancellationToken)
		{
			DateTime date = await this.ReadDateAsync(cancellationToken).ConfigureAwait(false);
			TimeSpan time = await this.ReadTimeAsync(cancellationToken).ConfigureAwait(false);

			return new DateTime(date.Year, date.Month, date.Day, time.Hours, time.Minutes, time.Seconds, time.Milliseconds);
		}

		public DateTime ReadDate()
		{
			return TypeDecoder.DecodeDate(this.ReadInt32());
		}
		public async Task<DateTime> ReadDateAsync(CancellationToken cancellationToken)
		{
			return TypeDecoder.DecodeDate(await this.ReadInt32Async(cancellationToken).ConfigureAwait(false));
		}

		public TimeSpan ReadTime()
		{
			return TypeDecoder.DecodeTime(this.ReadInt32());
		}
		public async Task<TimeSpan> ReadTimeAsync(CancellationToken cancellationToken)
		{
			return TypeDecoder.DecodeTime(await this.ReadInt32Async(cancellationToken).ConfigureAwait(false));
		}

		public decimal ReadDecimal(int type, int scale)
		{
			decimal value = 0;

			switch (type & ~1)
			{
				case IscCodes.SQL_SHORT:
					value = TypeDecoder.DecodeDecimal(this.ReadInt16(), scale, type);
					break;

				case IscCodes.SQL_LONG:
					value = TypeDecoder.DecodeDecimal(this.ReadInt32(), scale, type);
					break;

				case IscCodes.SQL_QUAD:
				case IscCodes.SQL_INT64:
					value = TypeDecoder.DecodeDecimal(this.ReadInt64(), scale, type);
					break;

				case IscCodes.SQL_DOUBLE:
				case IscCodes.SQL_D_FLOAT:
					value = Convert.ToDecimal(this.ReadDouble());
					break;
			}

			return value;
		}
		public async Task<decimal> ReadDecimalAsync(int type, int scale, CancellationToken cancellationToken)
		{
			decimal value = 0;

			switch (type & ~1)
			{
				case IscCodes.SQL_SHORT:
					value = TypeDecoder.DecodeDecimal(await this.ReadInt16Async(cancellationToken).ConfigureAwait(false), scale, type);
					break;

				case IscCodes.SQL_LONG:
					value = TypeDecoder.DecodeDecimal(await this.ReadInt32Async(cancellationToken).ConfigureAwait(false), scale, type);
					break;

				case IscCodes.SQL_QUAD:
				case IscCodes.SQL_INT64:
					value = TypeDecoder.DecodeDecimal(await this.ReadInt64Async(cancellationToken).ConfigureAwait(false), scale, type);
					break;

				case IscCodes.SQL_DOUBLE:
				case IscCodes.SQL_D_FLOAT:
					value = Convert.ToDecimal(await this.ReadDoubleAsync(cancellationToken).ConfigureAwait(false));
					break;
			}

			return value;
		}

		public object ReadValue(DbField field)
		{
			object fieldValue = null;
			Charset innerCharset = (this.charset.Name != "NONE") ? this.charset : field.Charset;

			switch (field.DbDataType)
			{
				case DbDataType.Char:
					if (field.Charset.IsOctetsCharset)
					{
						fieldValue = this.ReadOpaque(field.Length);
					}
					else
					{
						string s = this.ReadString(innerCharset, field.Length);

						if ((field.Length % field.Charset.BytesPerCharacter) == 0 &&
							s.Length > field.CharCount)
						{
							fieldValue = s.Substring(0, field.CharCount);
						}
						else
						{
							fieldValue = s;
						}
					}
					break;

				case DbDataType.VarChar:
					if (field.Charset.IsOctetsCharset)
					{
						fieldValue = this.ReadBuffer();
					}
					else
					{
						fieldValue = this.ReadString(innerCharset);
					}
					break;

				case DbDataType.SmallInt:
					fieldValue = this.ReadInt16();
					break;

				case DbDataType.Integer:
					fieldValue = this.ReadInt32();
					break;

				case DbDataType.Array:
				case DbDataType.Binary:
				case DbDataType.Text:
				case DbDataType.BigInt:
					fieldValue = this.ReadInt64();
					break;

				case DbDataType.Decimal:
				case DbDataType.Numeric:
					fieldValue = this.ReadDecimal(field.DataType, field.NumericScale);
					break;

				case DbDataType.Float:
					fieldValue = this.ReadSingle();
					break;

				case DbDataType.Guid:
					fieldValue = this.ReadGuid(field.Length);
					break;

				case DbDataType.Double:
					fieldValue = this.ReadDouble();
					break;

				case DbDataType.Date:
					fieldValue = this.ReadDate();
					break;

				case DbDataType.Time:
					fieldValue = this.ReadTime();
					break;

				case DbDataType.TimeStamp:
					fieldValue = this.ReadDateTime();
					break;
			}

			int sqlInd = this.ReadInt32();

			if (sqlInd == 0)
			{
				return fieldValue;
			}
			else if (sqlInd == -1)
			{
				return null;
			}
			else
			{
				throw new IscException("invalid sqlind value: " + sqlInd);
			}
		}
		public async Task<object> ReadValueAsync(DbField field, CancellationToken cancellationToken)
		{
			object fieldValue = null;
			Charset innerCharset = (this.charset.Name != "NONE") ? this.charset : field.Charset;

			switch (field.DbDataType)
			{
				case DbDataType.Char:
					if (field.Charset.IsOctetsCharset)
					{
						fieldValue = await this.ReadOpaqueAsync(field.Length, cancellationToken).ConfigureAwait(false);
					}
					else
					{
						string s = await this.ReadStringAsync(innerCharset, field.Length, cancellationToken).ConfigureAwait(false);

						if ((field.Length % field.Charset.BytesPerCharacter) == 0 &&
							s.Length > field.CharCount)
						{
							fieldValue = s.Substring(0, field.CharCount);
						}
						else
						{
							fieldValue = s;
						}
					}
					break;

				case DbDataType.VarChar:
					if (field.Charset.IsOctetsCharset)
					{
						fieldValue = await this.ReadBufferAsync(cancellationToken).ConfigureAwait(false);
					}
					else
					{
						fieldValue = await this.ReadStringAsync(innerCharset, cancellationToken).ConfigureAwait(false);
					}
					break;

				case DbDataType.SmallInt:
					fieldValue = await this.ReadInt16Async(cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.Integer:
					fieldValue = await this.ReadInt32Async(cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.Array:
				case DbDataType.Binary:
				case DbDataType.Text:
				case DbDataType.BigInt:
					fieldValue = await this.ReadInt64Async(cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.Decimal:
				case DbDataType.Numeric:
					fieldValue = await this.ReadDecimalAsync(field.DataType, field.NumericScale, cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.Float:
					fieldValue = await this.ReadSingleAsync(cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.Guid:
					fieldValue = await this.ReadGuidAsync(field.Length, cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.Double:
					fieldValue = await this.ReadDoubleAsync(cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.Date:
					fieldValue = await this.ReadDateAsync(cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.Time:
					fieldValue = await this.ReadTimeAsync(cancellationToken).ConfigureAwait(false);
					break;

				case DbDataType.TimeStamp:
					fieldValue = await this.ReadDateTimeAsync(cancellationToken).ConfigureAwait(false);
					break;
			}

			int sqlInd = await this.ReadInt32Async(cancellationToken).ConfigureAwait(false);

			if (sqlInd == 0)
			{
				return fieldValue;
			}
			else if (sqlInd == -1)
			{
				return null;
			}
			else
			{
				throw new IscException("invalid sqlind value: " + sqlInd);
			}
		}

		#endregion

		#region · XDR Write Methods ·

		public void WriteOpaque(byte[] buffer)
		{
			this.WriteOpaque(buffer, buffer.Length);
		}
		public Task WriteOpaqueAsync(byte[] buffer, CancellationToken cancellationToken)
		{
			return this.WriteOpaqueAsync(buffer, buffer.Length, cancellationToken);
		}

		public void WriteOpaque(byte[] buffer, int length)
		{
			if (buffer != null && length > 0)
			{
				this.Write(buffer, 0, buffer.Length);
				this.Write(Fill, 0, length - buffer.Length);
				this.Write(Pad, 0, ((4 - length) & 3));
			}
		}
		public async Task WriteOpaqueAsync(byte[] buffer, int length, CancellationToken cancellationToken)
		{
			if (buffer != null && length > 0)
			{
				await this.WriteAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);
				await this.WriteAsync(Fill, 0, length - buffer.Length, cancellationToken).ConfigureAwait(false);
				await this.WriteAsync(Pad, 0, ((4 - length) & 3), cancellationToken).ConfigureAwait(false);
			}
		}

		public void WriteBuffer(byte[] buffer)
		{
			this.WriteBuffer(buffer, buffer == null ? 0 : buffer.Length);
		}
		public Task WriteBufferAsync(byte[] buffer, CancellationToken cancellationToken)
		{
			return this.WriteBufferAsync(buffer, buffer == null ? 0 : buffer.Length, cancellationToken);
		}

		public void WriteBuffer(byte[] buffer, int length)
		{
			this.Write(length);

			if (buffer != null && length > 0)
			{
				this.Write(buffer, 0, length);
				this.Write(Pad, 0, ((4 - length) & 3));
			}
		}
		public async Task WriteBufferAsync(byte[] buffer, int length, CancellationToken cancellationToken)
		{
			await this.WriteAsync(length, cancellationToken).ConfigureAwait(false);

			if (buffer != null && length > 0)
			{
				await this.WriteAsync(buffer, 0, length, cancellationToken).ConfigureAwait(false);
				await this.WriteAsync(Pad, 0, ((4 - length) & 3), cancellationToken).ConfigureAwait(false);
			}
		}

		public void WriteBlobBuffer(byte[] buffer)
		{
			int length = buffer.Length;	// 2 for short for buffer length

			if (length > short.MaxValue)
			{
				throw (new IOException()); //Need a	value???
			}

			this.Write(length + 2);
			this.Write(length + 2);	//bizarre but true!	three copies of	the	length
			this.WriteByte((byte)((length >> 0) & 0xff));
			this.WriteByte((byte)((length >> 8) & 0xff));
			this.Write(buffer, 0, length);

			this.Write(Pad, 0, ((4 - length + 2) & 3));
		}
		public async Task WriteBlobBufferAsync(byte[] buffer, CancellationToken cancellationToken)
		{
			int length = buffer.Length;	// 2 for short for buffer length

			if (length > short.MaxValue)
			{
				throw (new IOException()); //Need a	value???
			}

			await this.WriteAsync(length + 2, cancellationToken).ConfigureAwait(false);
			await this.WriteAsync(length + 2, cancellationToken).ConfigureAwait(false);	//bizarre but true!	three copies of	the	length
			await this.WriteByteAsync((byte)((length >> 0) & 0xff), cancellationToken).ConfigureAwait(false);
			await this.WriteByteAsync((byte)((length >> 8) & 0xff), cancellationToken).ConfigureAwait(false);
			await this.WriteAsync(buffer, 0, length, cancellationToken).ConfigureAwait(false);

			await this.WriteAsync(Pad, 0, ((4 - length + 2) & 3), cancellationToken).ConfigureAwait(false);
		}

		public void WriteTyped(int type, byte[] buffer)
		{
			int length;

			if (buffer == null)
			{
				this.Write(1);
				this.WriteByte((byte)type);
				length = 1;
			}
			else
			{
				length = buffer.Length + 1;
				this.Write(length);
				this.WriteByte((byte)type);
				this.Write(buffer, 0, buffer.Length);
			}
			this.Write(Pad, 0, ((4 - length) & 3));
		}
		public async Task WriteTypedAsync(int type, byte[] buffer, CancellationToken cancellationToken)
		{
			int length;

			if (buffer == null)
			{
				await this.WriteAsync(1, cancellationToken).ConfigureAwait(false);
				await this.WriteByteAsync((byte)type, cancellationToken).ConfigureAwait(false);
				length = 1;
			}
			else
			{
				length = buffer.Length + 1;
				await this.WriteAsync(length, cancellationToken).ConfigureAwait(false);
				await this.WriteByteAsync((byte)type, cancellationToken).ConfigureAwait(false);
				await this.WriteAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);
			}
			await this.WriteAsync(Pad, 0, ((4 - length) & 3), cancellationToken).ConfigureAwait(false);
		}

		public void Write(string value)
		{
			byte[] buffer = this.charset.GetBytes(value);

			this.WriteBuffer(buffer, buffer.Length);
		}
		public Task WriteAsync(string value, CancellationToken cancellationToken)
		{
			byte[] buffer = this.charset.GetBytes(value);

			return this.WriteBufferAsync(buffer, buffer.Length, cancellationToken);
		}

		public void Write(short value)
		{
			this.Write((int)value);
		}
		public Task WriteAsync(short value, CancellationToken cancellationToken)
		{
			return this.WriteAsync((int)value, cancellationToken);
		}

		public void Write(int value)
		{
			this.Write(BitConverter.GetBytes(IPAddress.NetworkToHostOrder(value)), 0, 4);
		}
		public Task WriteAsync(int value, CancellationToken cancellationToken)
		{
			return this.WriteAsync(BitConverter.GetBytes(IPAddress.NetworkToHostOrder(value)), 0, 4, cancellationToken);
		}

		public void Write(long value)
		{
			this.Write(BitConverter.GetBytes(IPAddress.NetworkToHostOrder(value)), 0, 8);
		}
		public Task WriteAsync(long value, CancellationToken cancellationToken)
		{
			return this.WriteAsync(BitConverter.GetBytes(IPAddress.NetworkToHostOrder(value)), 0, 8, cancellationToken);
		}

		public void Write(float value)
		{
			byte[] buffer = BitConverter.GetBytes(value);

			this.Write(BitConverter.ToInt32(buffer, 0));
		}
		public Task WriteAsync(float value, CancellationToken cancellationToken)
		{
			byte[] buffer = BitConverter.GetBytes(value);

			return this.WriteAsync(BitConverter.ToInt32(buffer, 0), cancellationToken);
		}

		public void Write(double value)
		{
			byte[] buffer = BitConverter.GetBytes(value);

			this.Write(BitConverter.ToInt64(buffer, 0));
		}
		public Task WriteAsync(double value, CancellationToken cancellationToken)
		{
			byte[] buffer = BitConverter.GetBytes(value);

			return this.WriteAsync(BitConverter.ToInt64(buffer, 0), cancellationToken);
		}

		public void Write(decimal value, int type, int scale)
		{
			object numeric = TypeEncoder.EncodeDecimal(value, scale, type);

			switch (type & ~1)
			{
				case IscCodes.SQL_SHORT:
					this.Write((short)numeric);
					break;

				case IscCodes.SQL_LONG:
					this.Write((int)numeric);
					break;

				case IscCodes.SQL_QUAD:
				case IscCodes.SQL_INT64:
					this.Write((long)numeric);
					break;

				case IscCodes.SQL_DOUBLE:
				case IscCodes.SQL_D_FLOAT:
					this.Write((double)value);
					break;
			}
		}
		public async Task WriteAsync(decimal value, int type, int scale, CancellationToken cancellationToken)
		{
			object numeric = TypeEncoder.EncodeDecimal(value, scale, type);

			switch (type & ~1)
			{
				case IscCodes.SQL_SHORT:
					await this.WriteAsync((short)numeric, cancellationToken);
					break;

				case IscCodes.SQL_LONG:
					await this.WriteAsync((int)numeric, cancellationToken);
					break;

				case IscCodes.SQL_QUAD:
				case IscCodes.SQL_INT64:
					await this.WriteAsync((long)numeric, cancellationToken);
					break;

				case IscCodes.SQL_DOUBLE:
				case IscCodes.SQL_D_FLOAT:
					await this.WriteAsync((double)value, cancellationToken);
					break;
			}
		}

		public void Write(bool value)
		{
			this.Write((short)(value ? 1 : 0));
		}
		public Task WriteAsync(bool value, CancellationToken cancellationToken)
		{
			return this.WriteAsync((short)(value ? 1 : 0), cancellationToken);
		}

		public void Write(DateTime value)
		{
			this.WriteDate(value);
			this.WriteTime(TypeHelper.DateTimeToTimeSpan(value));
		}
		public async Task WriteAsync(DateTime value, CancellationToken cancellationToken)
		{
			await this.WriteDateAsync(value, cancellationToken).ConfigureAwait(false);
			await this.WriteTimeAsync(TypeHelper.DateTimeToTimeSpan(value), cancellationToken).ConfigureAwait(false);
		}

		public void WriteDate(DateTime value)
		{
			this.Write(TypeEncoder.EncodeDate(Convert.ToDateTime(value)));
		}
		public Task WriteDateAsync(DateTime value, CancellationToken cancellationToken)
		{
			return this.WriteAsync(TypeEncoder.EncodeDate(Convert.ToDateTime(value)), cancellationToken);
		}

		public void WriteTime(TimeSpan value)
		{
			this.Write(TypeEncoder.EncodeTime(value));
		}
		public Task WriteTimeAsync(TimeSpan value, CancellationToken cancellationToken)
		{
			return this.WriteAsync(TypeEncoder.EncodeTime(value), cancellationToken);
		}

		public void Write(Descriptor descriptor)
		{
			for (int i = 0; i < descriptor.Count; i++)
			{
				this.Write(descriptor[i]);
			}
		}
		public async Task WriteAsync(Descriptor descriptor, CancellationToken cancellationToken)
		{
			for (int i = 0; i < descriptor.Count; i++)
			{
				await this.WriteAsync(descriptor[i], cancellationToken).ConfigureAwait(false);
			}
		}

		public void Write(DbField param)
		{
			try
			{
				if (param.DbDataType != DbDataType.Null)
				{
					param.FixNull();

					switch (param.DbDataType)
					{
						case DbDataType.Char:
							if (param.Charset.IsOctetsCharset)
							{
								this.WriteOpaque(param.DbValue.GetBinary(), param.Length);
							}
							else
							{
								string svalue = param.DbValue.GetString();

								if ((param.Length % param.Charset.BytesPerCharacter) == 0 &&
									svalue.Length > param.CharCount)
								{
									throw new IscException(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
								}

								this.WriteOpaque(param.Charset.GetBytes(svalue), param.Length);
							}
							break;

						case DbDataType.VarChar:
							if (param.Charset.IsOctetsCharset)
							{
								this.WriteOpaque(param.DbValue.GetBinary(), param.Length);
							}
							else
							{
								string svalue = param.DbValue.GetString();

								if ((param.Length % param.Charset.BytesPerCharacter) == 0 &&
									svalue.Length > param.CharCount)
								{
									throw new IscException(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
								}

								byte[] data = param.Charset.GetBytes(svalue);

								this.WriteBuffer(data, data.Length);
							}
							break;

						case DbDataType.SmallInt:
							this.Write(param.DbValue.GetInt16());
							break;

						case DbDataType.Integer:
							this.Write(param.DbValue.GetInt32());
							break;

						case DbDataType.BigInt:
						case DbDataType.Array:
						case DbDataType.Binary:
						case DbDataType.Text:
							this.Write(param.DbValue.GetInt64());
							break;

						case DbDataType.Decimal:
						case DbDataType.Numeric:
							this.Write(
								param.DbValue.GetDecimal(),
								param.DataType,
								param.NumericScale);
							break;

						case DbDataType.Float:
							this.Write(param.DbValue.GetFloat());
							break;

						case DbDataType.Guid:
							this.WriteOpaque(param.DbValue.GetGuid().ToByteArray());
							break;

						case DbDataType.Double:
							this.Write(param.DbValue.GetDouble());
							break;

						case DbDataType.Date:
							this.Write(param.DbValue.GetDate());
							break;

						case DbDataType.Time:
							this.Write(param.DbValue.GetTime());
							break;

						case DbDataType.TimeStamp:
							this.Write(param.DbValue.GetDate());
							this.Write(param.DbValue.GetTime());
							break;

						case DbDataType.Boolean:
							this.Write(Convert.ToBoolean(param.Value));
							break;

						default:
							throw new IscException("Unknown sql data type: " + param.DataType);
					}
				}

				this.Write(param.NullFlag);
			}
			catch (IOException)
			{
				throw new IscException(IscCodes.isc_net_write_err);
			}
		}
		public async Task WriteAsync(DbField param, CancellationToken cancellationToken)
		{
			try
			{
				if (param.DbDataType != DbDataType.Null)
				{
					param.FixNull();

					switch (param.DbDataType)
					{
						case DbDataType.Char:
							if (param.Charset.IsOctetsCharset)
							{
								await this.WriteOpaqueAsync(param.DbValue.GetBinary(), param.Length, cancellationToken).ConfigureAwait(false);
							}
							else
							{
								string svalue = param.DbValue.GetString();

								if ((param.Length % param.Charset.BytesPerCharacter) == 0 &&
									svalue.Length > param.CharCount)
								{
									throw new IscException(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
								}

								await this.WriteOpaqueAsync(param.Charset.GetBytes(svalue), param.Length, cancellationToken).ConfigureAwait(false);
							}
							break;

						case DbDataType.VarChar:
							if (param.Charset.IsOctetsCharset)
							{
								await this.WriteOpaqueAsync(param.DbValue.GetBinary(), param.Length, cancellationToken).ConfigureAwait(false);
							}
							else
							{
								string svalue = param.DbValue.GetString();

								if ((param.Length % param.Charset.BytesPerCharacter) == 0 &&
									svalue.Length > param.CharCount)
								{
									throw new IscException(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
								}

								byte[] data = param.Charset.GetBytes(svalue);

								await this.WriteBufferAsync(data, data.Length, cancellationToken).ConfigureAwait(false);
							}
							break;

						case DbDataType.SmallInt:
							await this.WriteAsync(param.DbValue.GetInt16(), cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.Integer:
							await this.WriteAsync(param.DbValue.GetInt32(), cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.BigInt:
						case DbDataType.Array:
						case DbDataType.Binary:
						case DbDataType.Text:
							await this.WriteAsync(param.DbValue.GetInt64(), cancellationToken).ConfigureAwait(false); ;
							break;

						case DbDataType.Decimal:
						case DbDataType.Numeric:
							await this.WriteAsync(
								param.DbValue.GetDecimal(),
								param.DataType,
								param.NumericScale,
								cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.Float:
							await this.WriteAsync(param.DbValue.GetFloat(), cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.Guid:
							await this.WriteOpaqueAsync(param.DbValue.GetGuid().ToByteArray(), cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.Double:
							await this.WriteAsync(param.DbValue.GetDouble(), cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.Date:
							await this.WriteAsync(param.DbValue.GetDate(), cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.Time:
							await this.WriteAsync(param.DbValue.GetTime(), cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.TimeStamp:
							await this.WriteAsync(param.DbValue.GetDate(), cancellationToken).ConfigureAwait(false);
							await this.WriteAsync(param.DbValue.GetTime(), cancellationToken).ConfigureAwait(false);
							break;

						case DbDataType.Boolean:
							await this.WriteAsync(Convert.ToBoolean(param.Value), cancellationToken).ConfigureAwait(false);
							break;

						default:
							throw new IscException("Unknown sql data type: " + param.DataType);
					}
				}

				await this.WriteAsync(param.NullFlag, cancellationToken).ConfigureAwait(false);
			}
			catch (IOException)
			{
				throw new IscException(IscCodes.isc_net_write_err);
			}
		}

		#endregion

		#region · Private Methods ·

		private void CheckDisposed()
		{
			if (this.innerStream == null)
			{
				throw new ObjectDisposedException("The XdrStream is closed.");
			}
		}

		private void ResetOperation()
		{
			this.operation = -1;
		}

		#endregion

		#region · Private Properties ·

		private bool ValidOperationAvailable
		{
			get { return this.operation >= 0; }
		}

		#endregion
	}
}
