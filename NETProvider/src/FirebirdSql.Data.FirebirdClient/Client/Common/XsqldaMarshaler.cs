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
using System.Runtime.InteropServices;
using System.Text;

using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Common
{
	internal sealed class XsqldaMarshaler
	{
		#region Static Fields

		private static readonly XsqldaMarshaler instance = new XsqldaMarshaler();

		#endregion

		#region Static Properties

		public static XsqldaMarshaler Instance
		{
			get { return XsqldaMarshaler.instance; }
		}

		#endregion

		#region Constructors

		private XsqldaMarshaler()
		{
		}

		#endregion

		#region Methods

		public void CleanUpNativeData(ref IntPtr pNativeData)
		{
			if (pNativeData != IntPtr.Zero)
			{
				// Obtain XSQLDA information
				XSQLDA xsqlda = new XSQLDA();

				xsqlda = (XSQLDA)Marshal.PtrToStructure(pNativeData, typeof(XSQLDA));

				// Destroy XSQLDA structure
				Marshal.DestroyStructure(pNativeData, typeof(XSQLDA));

				// Destroy XSQLVAR structures
				for (int i = 0; i < xsqlda.sqln; i++)
				{
					IntPtr ptr1 = this.GetIntPtr(pNativeData, this.ComputeLength(i));

					// Free	sqldata	and	sqlind pointers	if needed
					XSQLVAR sqlvar = (XSQLVAR)Marshal.PtrToStructure(ptr1, typeof(XSQLVAR));

					if (sqlvar.sqldata != IntPtr.Zero)
					{
						Marshal.FreeHGlobal(sqlvar.sqldata);
						sqlvar.sqldata = IntPtr.Zero;
					}
					if (sqlvar.sqlind != IntPtr.Zero)
					{
						Marshal.FreeHGlobal(sqlvar.sqlind);
						sqlvar.sqlind = IntPtr.Zero;
					}

					IntPtr ptr2 = this.GetIntPtr(pNativeData, this.ComputeLength(i));
					Marshal.DestroyStructure(ptr2, typeof(XSQLVAR));
				}

				// Free	pointer	memory
				Marshal.FreeHGlobal(pNativeData);

				pNativeData = IntPtr.Zero;
			}
		}

		public IntPtr MarshalManagedToNative(Charset charset, Descriptor descriptor)
		{
			// Set up XSQLDA structure
			XSQLDA xsqlda = new XSQLDA();

			xsqlda.version  = descriptor.Version;
			xsqlda.sqln     = descriptor.Count;
			xsqlda.sqld     = descriptor.ActualCount;

			XSQLVAR[] xsqlvar = new XSQLVAR[descriptor.Count];

			for (int i = 0; i < xsqlvar.Length; i++)
			{
				// Create a	new	XSQLVAR	structure and fill it
				xsqlvar[i] = new XSQLVAR();

				xsqlvar[i].sqltype      = descriptor[i].DataType;
				xsqlvar[i].sqlscale     = descriptor[i].NumericScale;
				xsqlvar[i].sqlsubtype   = descriptor[i].SubType;
				xsqlvar[i].sqllen       = descriptor[i].Length;

				// Create a	new	pointer	for	the	xsqlvar	data
				if (descriptor[i].HasDataType() && descriptor[i].DbDataType != DbDataType.Null)
				{
					byte[] buffer = descriptor[i].DbValue.GetBytes();
					xsqlvar[i].sqldata = Marshal.AllocHGlobal(buffer.Length);
					Marshal.Copy(buffer, 0, xsqlvar[i].sqldata, buffer.Length);
				}
				else
				{
					xsqlvar[i].sqldata = Marshal.AllocHGlobal(0);
				}

				// Create a	new	pointer	for	the	sqlind value
				xsqlvar[i].sqlind = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int16)));
				Marshal.WriteInt16(xsqlvar[i].sqlind, descriptor[i].NullFlag);

				// Name
				xsqlvar[i].sqlname = this.GetStringBuffer(charset, descriptor[i].Name);
				xsqlvar[i].sqlname_length = (short)descriptor[i].Name.Length;

				// Relation	Name
				xsqlvar[i].relname = this.GetStringBuffer(charset, descriptor[i].Relation);
				xsqlvar[i].relname_length = (short)descriptor[i].Relation.Length;

				// Owner name
				xsqlvar[i].ownername = this.GetStringBuffer(charset, descriptor[i].Owner);
				xsqlvar[i].ownername_length = (short)descriptor[i].Owner.Length;

				// Alias name
				xsqlvar[i].aliasname = this.GetStringBuffer(charset, descriptor[i].Alias);
				xsqlvar[i].aliasname_length = (short)descriptor[i].Alias.Length;
			}

			return this.MarshalManagedToNative(xsqlda, xsqlvar);
		}

		public IntPtr MarshalManagedToNative(XSQLDA xsqlda, XSQLVAR[] xsqlvar)
		{
			int size = this.ComputeLength(xsqlda.sqln);
			IntPtr ptr = Marshal.AllocHGlobal(size);

			Marshal.StructureToPtr(xsqlda, ptr, true);

			for (int i = 0; i < xsqlvar.Length; i++)
			{
				int offset = this.ComputeLength(i);
				Marshal.StructureToPtr(xsqlvar[i], this.GetIntPtr(ptr, offset), true);
			}

			return ptr;
		}
		
		static int sqlTypeOffset = Marshal.OffsetOf(typeof(XSQLVAR), "sqltype").AsInt();
		static int sqlscaleOffset = Marshal.OffsetOf(typeof(XSQLVAR), "sqlscale").AsInt();
		static int sqlsubtypeOffset = Marshal.OffsetOf(typeof(XSQLVAR), "sqlsubtype").AsInt();
		static int sqllenOffset = Marshal.OffsetOf(typeof(XSQLVAR), "sqllen").AsInt();
		static int sqldataOffset = Marshal.OffsetOf(typeof(XSQLVAR), "sqldata").AsInt();
		static int sqlindOffset = Marshal.OffsetOf(typeof(XSQLVAR), "sqlind").AsInt();
		static int sqlname_lengthOffset = Marshal.OffsetOf(typeof(XSQLVAR), "sqlname_length").AsInt();
		static int sqlnameOffset = Marshal.OffsetOf(typeof(XSQLVAR), "sqlname").AsInt();
		static int relname_lengthOffset = Marshal.OffsetOf(typeof(XSQLVAR), "relname_length").AsInt();
		static int relnameOffset = Marshal.OffsetOf(typeof(XSQLVAR), "relname").AsInt();
		static int ownername_lengthOffset = Marshal.OffsetOf(typeof(XSQLVAR), "ownername_length").AsInt();
		static int ownernameOffset = Marshal.OffsetOf(typeof(XSQLVAR), "ownername").AsInt();
		static int aliasname_lengthOffset = Marshal.OffsetOf(typeof(XSQLVAR), "aliasname_length").AsInt();
		static int aliasnameOffset = Marshal.OffsetOf(typeof(XSQLVAR), "aliasname").AsInt();


		static void MarshalVAR(IntPtr ptr, XSQLVAR var)
		{
			unsafe
			{
				var.sqltype = *(short*)(ptr + sqlTypeOffset);
				var.sqlscale = *(short*)(ptr + sqlscaleOffset);
				var.sqlsubtype = *(short*)(ptr + sqlsubtypeOffset);
				var.sqllen = *(short*)(ptr + sqllenOffset);
				var.sqldata = *(IntPtr*)(ptr + sqldataOffset);
				var.sqlind = *(IntPtr*)(ptr + sqlindOffset);
				var.sqlname_length = *(short*)(ptr + sqlname_lengthOffset);
				Marshal.Copy((ptr + sqlnameOffset), var.sqlname, 0, 32);
				var.relname_length = *(short*)(ptr + relname_lengthOffset);
				Marshal.Copy((ptr + relnameOffset), var.relname, 0, 32);
				var.ownername_length = *(short*)(ptr + ownername_lengthOffset);
				Marshal.Copy((ptr + ownernameOffset), var.ownername, 0, 32);
				var.aliasname_length = *(short*)(ptr + aliasname_lengthOffset);
				Marshal.Copy((ptr + aliasnameOffset), var.aliasname, 0, 32);
			}
		}

		public Descriptor MarshalNativeToManaged(Charset charset, IntPtr pNativeData)
		{
			return this.MarshalNativeToManaged(charset, pNativeData, false);
		}

		public Descriptor MarshalNativeToManaged(Charset charset, IntPtr pNativeData, bool fetching)
		{
			// Obtain XSQLDA information
			XSQLDA xsqlda = new XSQLDA();

			xsqlda = (XSQLDA)Marshal.PtrToStructure(pNativeData, typeof(XSQLDA));

			// Create a	new	Descriptor
			Descriptor descriptor   = new Descriptor(xsqlda.sqln);
			descriptor.ActualCount  = xsqlda.sqld;

			// Obtain XSQLVAR members information

			XSQLVAR xsqlvar = new XSQLVAR();
			for (var i = 0; i < xsqlda.sqln; i++)
			{
				IntPtr ptr = this.GetIntPtr(pNativeData, this.ComputeLength(i));
				MarshalVAR(ptr, xsqlvar);

				// Map XSQLVAR information to Descriptor
				descriptor[i].DataType = xsqlvar.sqltype;
				descriptor[i].NumericScale = xsqlvar.sqlscale;
				descriptor[i].SubType = xsqlvar.sqlsubtype;
				descriptor[i].Length = xsqlvar.sqllen;

				// Decode sqlind value
				if (xsqlvar.sqlind == IntPtr.Zero)
				{
					descriptor[i].NullFlag = 0;
				}
				else
				{
					descriptor[i].NullFlag = Marshal.ReadInt16(xsqlvar.sqlind);
				}

				// Set value
				if (fetching)
				{
					if (descriptor[i].NullFlag != -1)
					{
						descriptor[i].SetValue(this.GetBytes(xsqlvar));
					}
				}

				descriptor[i].Name = GetString(charset, xsqlvar.sqlname, xsqlvar.sqlname_length);
				descriptor[i].Relation = GetString(charset, xsqlvar.relname, xsqlvar.relname_length);
				descriptor[i].Owner = GetString(charset, xsqlvar.ownername, xsqlvar.ownername_length);
				descriptor[i].Alias = GetString(charset, xsqlvar.aliasname, xsqlvar.aliasname_length);
			}

			return descriptor;
		}

		#endregion

		#region Private Methods

		private IntPtr GetIntPtr(IntPtr ptr, int offset)
		{
			return new IntPtr(ptr.ToInt64() + offset);
		}

		static int sizeofXSQLDA = Marshal.SizeOf(typeof(XSQLDA));
		static int sizeofXSQLVAR = Marshal.SizeOf(typeof(XSQLVAR));

		private int ComputeLength(int n)
		{
			var length = (sizeofXSQLDA + n * sizeofXSQLVAR);
			if (IntPtr.Size == 8)
				length += 4;
			return length;
		}

		private byte[] GetBytes(XSQLVAR xsqlvar)
		{
			byte[] buffer   = null;
			IntPtr tmp      = IntPtr.Zero;

			if (xsqlvar.sqllen == 0 || xsqlvar.sqldata == IntPtr.Zero)
			{
				return null;
			}

			switch (xsqlvar.sqltype & ~1)
			{
				case IscCodes.SQL_VARYING:
					buffer  = new byte[Marshal.ReadInt16(xsqlvar.sqldata)];
					tmp     = this.GetIntPtr(xsqlvar.sqldata, 2);

					Marshal.Copy(tmp, buffer, 0, buffer.Length);

					return buffer;

				case IscCodes.SQL_TEXT:
				case IscCodes.SQL_SHORT:
				case IscCodes.SQL_LONG:
				case IscCodes.SQL_FLOAT:
				case IscCodes.SQL_DOUBLE:
				case IscCodes.SQL_D_FLOAT:
				case IscCodes.SQL_QUAD:
				case IscCodes.SQL_INT64:
				case IscCodes.SQL_BLOB:
				case IscCodes.SQL_ARRAY:
				case IscCodes.SQL_TIMESTAMP:
				case IscCodes.SQL_TYPE_TIME:
				case IscCodes.SQL_TYPE_DATE:
					buffer = new byte[xsqlvar.sqllen];
					Marshal.Copy(xsqlvar.sqldata, buffer, 0, buffer.Length);

					return buffer;

				default:
					throw new NotSupportedException("Unknown data type");
			}
		}

		private byte[] GetStringBuffer(Charset charset, string value)
		{
			byte[] buffer = new byte[32];

			charset.GetBytes(value, 0, value.Length, buffer, 0);

			return buffer;
		}

		private string GetString(Charset charset, byte[] buffer)
		{
			string value = charset.GetString(buffer);

			return value.TrimEnd('\0', ' ');
		}

		private static string GetString(Charset charset, byte[] buffer, short bufferLength)
		{
			return charset.GetString(buffer, 0, bufferLength);
		}

		#endregion
	}
}
