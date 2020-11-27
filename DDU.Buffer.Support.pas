unit DDU.Buffer.Support;

//*****************************************************************************
//
// DDUINET (DDU.Buffer.Support)
// Copyright 2020 Clinton R. Johnson (xepol@xepol.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Version : 1.0
//
// Purpose : Support routines for DDU.Buffer
//
// History : <none>
//
//*****************************************************************************

interface

{$I dver.inc}

Uses
  System.SysUtils,
  System.Classes;

{$I dTypes.inc}

Type
  TBytess = Array[0..0] Of Byte;
  PBytes = ^TBytess;

Type
  TStringMode  = (smAnsi,smUTF8, smUTF16, smRaw);


{ Convert datatypes to TBytes arrays }
Function bBoolean(V : Boolean) : TBytes;
Function bByte(V : Byte) : TBytes;
Function bBytes(Const V : Array Of TBytes) : TBytes;
Function bChar(V : Char) : TBytes;
Function bFloat32(V: Single) : TBytes;
Function bFloat64(V: Double) : TBytes;
Function bInteger(V : Int64) : TBytes;
Function bString(Const V : String; Const StringMode : TStringMode=smUTF8) : TBytes; Overload;
Function bVarRec(V : TVarRec; StringMode : TStringMode=smUTF8) : TBytes;
Function Bytes(Const V : Array Of Const; StringMode : TStringMode=smUTF8) : TBytes;

{ Convert datatypes to Strings }
Function sBytes(Const Bytes : TBytes; Const StringMode : TStringMode=smUTF8) : String;
Function wBytes(Const Bytes : TBytes; Const StringMode : TStringMode=smUTF8) : WideString; {$IF defined(Unicode)}Deprecated;{$ENDIF}

{ Convert datatypes to printable String }
Function printableBytes(S : TBytes; AllHex : Boolean=False) : String; Overload;
Function printableBytes(Const S : String; AllHex : Boolean=False) : String; Overload;

{$IF not defined(NEXTGEN)}

{$IF defined(Unicode)}
Function bString(Const V : AnsiString; Const StringMode : TStringMode=smUTF8) : TBytes; Overload;
Function printableBytes(Const S : AnsiString; AllHex : Boolean=False) : String; Overload;
{$Else}
Function bString(Const V : WideString; Const StringMode : TStringMode=smUTF8) : TBytes; Overload;
Function printableBytes(Const S : WideString; AllHex : Boolean=False) : String; Overload;
{$EndIf}

{$ENDIF}

{ Byte functions }
Function SameBytes(B1,B2 : TBytes) : Boolean;
Procedure bAppend(Var Bytes : TBytes; ToAppend : TBytes);
Function bCopy(Const Bytes : TBytes; Start : Integer; Count : Integer) : TBytes;
Procedure bDelete(Var Bytes : TBytes; Start : Integer; Count : Integer);

Var
  __CRLF : TBytes;
  __LFCR : TBytes;
  __CR   : TBytes;
  __LF   : TBytes;


implementation


{$REGION 'TBytes Handling'}
Type
  TByteMap=Packed Record
    Procedure Clear;
    Function ToBytes(count : Integer=8) : TBytes;
    Function ByteCount : Integer;
    Function AltByteCount : Integer;
    Procedure Test;
  Case Int64 Of
    1 : (Bytes : Packed Array [0..7] Of Byte);
    2 : (U64 : UInt64);
    3 : (U32 : UInt32);
    4 : (U16 : UInt16);
    5 : (U8  : UInt8);
    6 : (I64 : Int64);
    7 : (I32 : Int32);
    8 : (I16 : Int16);
    9 : (I8  : Int8);
    10 : (By : Byte);
    11 : (Single : Single);
    12 : (Double : Double);
  End;

{ TBytemap }
function  TByteMap.AltByteCount: Integer;

Var
  Loop                    : Integer;

Begin
  Result := 1;
  For Loop := High(Bytes) DownTo Low(Bytes)+1 Do
  Begin
    If (Bytes[Loop] And $ff)<>0 Then
    Begin
      Result := Loop+1;
      Break;
    End;
  End;
End;
Function  TByteMap.ByteCount : Integer;

Begin
  If (Bytes[7] And $80)=$80 Then // Top bit, 2's compliment sign extended negative
  Begin
    If (Not U64 And $7FFFFFFF80000000)<>0 Then Result := 8 Else
    If (Not U32 And $7FFF8000)<>0         Then Result := 4 Else
    If (Not U16 And $7F80)<>0             Then Result := 2 Else
    Result := 1;
  End
  Else
  Begin
    If (U64 And $7FFFFFFF80000000)<>0 Then Result := 8 Else
    If (U32 And $7FFF8000)<>0         Then Result := 4 Else
    If (U16 And $7F80)<>0             Then Result := 2 Else
    Result := 1;
  End;
End;
Procedure TByteMap.Clear;

Begin
  I64 := 0;
End;
procedure TByteMap.Test;
begin
  I64 := 0; Assert(ByteCount=1,'Failed '+IntToStr(i64));
  I64 := -1; Assert(ByteCount=1,'Failed '+IntToStr(i64));
  I64 := -128; Assert(ByteCount=1,'Failed '+IntToStr(i64));
  I64 := -129; Assert(ByteCount=2,'Failed '+IntToStr(i64));
  I64 := -32768; Assert(ByteCount=2,'Failed '+IntToStr(i64));
  I64 := -32769; Assert(ByteCount=4,'Failed '+IntToStr(i64));
  I64 := -2147483648; Assert(ByteCount=4,'Failed '+IntToStr(i64));
  I64 := -2147483649; Assert(ByteCount=8,'Failed '+IntToStr(i64));
  I64 := -9223372036854775808; Assert(ByteCount=8,'Failed '+IntToStr(i64));


  I64 := 1; Assert(ByteCount=1,'Failed '+IntToStr(i64));
  I64 := 127; Assert(ByteCount=1,'Failed '+IntToStr(i64));
  I64 := 128; Assert(ByteCount=2,'Failed '+IntToStr(i64));
  I64 := 32767; Assert(ByteCount=2,'Failed '+IntToStr(i64));
  I64 := 32768; Assert(ByteCount=4,'Failed '+IntToStr(i64));
  I64 := 2147483647; Assert(ByteCount=4,'Failed '+IntToStr(i64));
  I64 := 2147483648; Assert(ByteCount=8,'Failed '+IntToStr(i64));
  I64 := 9223372036854775807; Assert(ByteCount=8,'Failed '+IntToStr(i64));
end;
Function  TByteMap.ToBytes(count : Integer=8) : TBytes;

Var
  Loop                    : Integer;

Begin
  SetLength(Result, Count);
// Little endian logic
  For Loop := 0 To Count-1 Do
  Begin
    Result[Loop] := Bytes[Count-Loop-1];
  End;
End;

{ Used to force the compiler to use a particular data type, otherwise immediates are mangled }
Function aSingle(S : Single) : Single;

Begin
  Result := S;
End;
Function aDouble(D : Double) : Double;
Begin
  Result := D;
End;

{ Convert datatypes to TBytes arrays }
Function bBoolean(V : Boolean) : TBytes;

Begin
  SetLength(Result,1);
  Case V Of
    True  : Result[Low(Result)] := 1;
    False : Result[Low(Result)] := 0;
  End;
End;
Function bByte(V : Byte) : TBytes;

Begin
  SetLength(Result,1);
  Result[Low(Result)] := V;
End;
Function bBytes(Const V : Array Of TBytes) : TBytes;

Var
  Loop                    : Integer;
  Len                     : Integer;
  At                      : Integer;
  B                       : TBytes;

Begin
  Len := 0;
  For Loop := Low(V) To High(V) Do
  Begin
    Inc(Len,Length(V[Loop]));
  End;
  SetLength(Result,Len);

  At := Low(result);
  For Loop := Low(V) To High(V) Do
  Begin
    B := V[Loop];
    Len := Length(B);
    Move(B[Low(B)],Result[At],Len);
    Inc(At,Len);
  End;
End;
Function bChar(V : Char) : TBytes;

Begin
  SetLength(Result,1);
  Result[Low(result)] := Byte(Ord(V));
End;
Function bFloat64(V: Double) : TBytes;

Var
  B                       : TByteMap;

Begin
  B.Double := V;
  Result := B.ToBytes(SizeOf(Double));
End;
Function bFloat32(V: Single) : TBytes;

Var
  B                       : TByteMap;

Begin
  B.Single := V;
  Result := B.ToBytes(SizeOf(Single));
End;
Function bInteger(V : Int64) : TBytes;

Var
  B                       : TByteMap;

Begin
  B.I64 := V;
  Result := B.ToBytes(B.AltByteCount);
End;

Function bString(Const V : String; Const StringMode : TStringMode=smUTF8) : TBytes;

Var
{$IF defined(UNICODE)}
  Encoding                : TEncoding;
{$ELSE}
  U                       : UTF8String;
{$IFEND}

Begin
{$IF defined(UNICODE)}
  If V='' Then Exit;
  Case StringMode Of
    smAnsi : Encoding := TEncoding.ANSI;
    smUTF8 : Encoding := TEncoding.UTF8;
  Else
    Encoding := TEncoding.ASCII;
  End;
  Result := System.SysUtils.TEncoding.Convert( System.SysUtils.TEncoding.Unicode, Encoding,
            System.SysUtils.TEncoding.Unicode.GetBytes(V));

{$ELSE}
  Case StringMode Of
    smUTF8  : Begin
                U := System.AnsiToUtf8(V);
                SetLength(Result,Length(U));
                Move(U[1],Result[Low(Result)],Length(Result));
              End;
    smRaw,
    smAnsi  : Begin
                SetLength(Result,Length(V));
                Move(V[1], Result[Low(Result)], Length(Result));
              End;
  Else
    SetLength(Result,0);
  End;
{$IFEND}
End;
Function bVarRec(V : TVarRec;StringMode : TStringMode=smUTF8) : TBytes;

Begin
  Case V.VType Of
// Normal types
    vtBoolean    : Result := bBoolean(V.VBoolean);
    vtCurrency   : Result := bFloat64(V.VCurrency^);
    vtExtended   : Result := bFloat64(V.VExtended^);
    vtInt64      : Result := bInteger(V.VInt64^);
    vtInteger    : Result := bInteger(V.VInteger);
    vtWideChar   : Result := bString(V.VWideChar,StringMode);
// String types
    vtPWideChar     : Result := bString(V.VPWideChar^ ,StringMode) ;
    vtWideString    : Result := bString( WideString(v.VWideString),StringMode );
{$IF defined(Unicode)}
    vtUnicodeString : Result := bString( String(V.VUnicodeString),StringMode );
{$ENDIF}
// Complex types
    vtClass      : SetLength(Result,0);
    vtInterface  : SetLength(Result,0);
    vtObject     : SetLength(Result,0);
    vtPointer    : SetLength(Result,0);
    vtVariant    : SetLength(Result,0);
// Old 8-bit string types
{$IFNDEF NEXTGEN}
    vtAnsiString : Result := bString(AnsiString(AnsiString(V.VAnsiString)),StringMode ); // AnsiString
    vtChar       : Result := bString(AnsiString(V.VChar),StringMode );
    vtPChar      : Result := bString(AnsiString(V.VPChar^),StringMode );
    vtString     : Result := bString(AnsiString(V.VString^),StringMode ); // Shortstring
{$Endif}
  Else
    SetLength(Result,0);
  End;
End;
Function Bytes(Const V : Array Of Const; StringMode : TStringMode) : TBytes;

Var
  Loop                    : Integer;

Begin
  SetLength(Result,0);
  For Loop := Low(V) To High(V) Do
  Begin
{$IF defined(DynamicArraySupport)}
    Result := Result+bVarRec(V[Loop],StringMode);
{$ELSE}
    bAppend(Result,bVarRec(V[Loop],StringMode));
{$ENDIF}
  End;
End;

{ Convert datatypes to Strings }

Function sBytes(Const Bytes : TBytes; Const StringMode : TStringMode=smUTF8) : String;

{$IFNDef Unicode}
Var
  U                       : UTF8String;
  W                       : WideString;
{$EndIf}


Begin
{$IF defined(Unicode)}
  Case StringMode Of
    smAnsi  : Result := System.SysUtils.TEncoding.Unicode.GetString(
                          System.SysUtils.TEncoding.Convert(System.SysUtils.TEncoding.ANSI,
                                                            System.SysUtils.TEncoding.Unicode,
                                                            Bytes));
    smUTF8  : Result := System.SysUtils.TEncoding.Unicode.GetString(
                          System.SysUtils.TEncoding.Convert(System.SysUtils.TEncoding.UTF8,
                                                            System.SysUtils.TEncoding.Unicode,
                                                            Bytes));
    smUTF16 : Result := System.SysUtils.TEncoding.Unicode.GetString(Bytes);
//    smUTF16 : Result := System.SysUtils.TEncoding.Unicode.GetString(
//                          System.SysUtils.TEncoding.Convert(System.SysUtils.TEncoding.Unicode,
//                                                            System.SysUtils.TEncoding.Unicode,
//                                                            Bytes));
  Else
    Result := '';
  End;
{$ELSE}
  Case StringMode Of
    smRaw,
    smAnsi  : Begin
                SetLength(Result,Length(Bytes));
                Move(Bytes[Low(Bytes)],Result[1],Length(Bytes));
              End;
    smUTF8  : Begin
                SetLength(U,Length(Bytes));
                Move(Bytes[Low(Bytes)],U[1],Length(Bytes));
                Result := Utf8ToAnsi(U);
              End;
  Else
    Result := '';
  End;
{$ENDIF}
End;
Function wBytes(Const Bytes : TBytes; Const StringMode : TStringMode=smUTF8) : WideString;

{$IFNDef Unicode}
Var
  A                       : AnsiString;
  U                       : UTF8String;
{$EndIf}

Begin
{$IF defined(Unicode)}
  Case StringMode Of
    smAnsi  : Result := System.SysUtils.TEncoding.Unicode.GetString(
                          System.SysUtils.TEncoding.Convert(System.SysUtils.TEncoding.ANSI,
                                                            System.SysUtils.TEncoding.Unicode,
                                                            Bytes));
    smUTF8  : Result := System.SysUtils.TEncoding.Unicode.GetString(
                          System.SysUtils.TEncoding.Convert(System.SysUtils.TEncoding.UTF8,
                                                            System.SysUtils.TEncoding.Unicode,
                                                            Bytes));
    smUTF16 : Result := System.SysUtils.TEncoding.Unicode.GetString(Bytes);
//    smUTF16 : Result := System.SysUtils.TEncoding.Unicode.GetString(
//                          System.SysUtils.TEncoding.Convert(System.SysUtils.TEncoding.Unicode,
//                                                            System.SysUtils.TEncoding.Unicode,
//                                                            Bytes));
  Else
    Result := '';
  End;
{$ELSE}
  Case StringMode Of
    smRaw,
    smAnsi : Begin
               SetLength(A,Length(Bytes));
               Move(Bytes[Low(Bytes)],A[1],Length(Bytes));
               Result := A;
             End;
    smUTF8 : Begin
               SetLength(U,Length(Bytes));
               Move(Bytes[Low(Bytes)],U[1],Length(Bytes));
               Result := UTF8Decode(U);
             End;
  Else
    Result := '';
  End;
{$ENDIF}
End;

Function printableBytes(S : TBytes; AllHex : Boolean=False) : String;

Var
  Loop                    : Integer;

Begin
  Result := '';
  For Loop := Low(S) TO High(S) Do
  Begin
    If (S[Loop]<32) Or (S[Loop]>$7f) Or AllHex Then
    Begin
      Result := Result+Format(' [%.2x] ',[S[Loop]]);
    End
    Else
    Begin
      Result := Result+ Char(S[Loop]);
    End;
  End;
End;
Function printableBytes(Const S : String; AllHex : Boolean=False) : String;

Var
  Loop                    : Integer;

Begin
  Result := '';
  For Loop := 1 To Length(S) Do
  Begin
    If (S[Loop]<#32) Or (S[Loop]>#$7f) Or AllHex Then
    Begin
      Result := Result+Format(' [%.2x] ',[ORd(S[Loop])]);
    End
    Else
    Begin
      Result := Result+ Char(S[Loop]);
    End;
  End;
End;

{ Byte functions }
Function SameBytes(B1,B2 : TBytes) : Boolean;

Var
  Loop                    : Integer;

Begin
  Result := False;
  If Length(B1)=Length(B2) Then
  Begin
    For Loop := Low(B1) To High(B1) Do
    Begin
      If B1[Loop]<>B2[Loop] Then
      Begin
        Exit;
      End;
    End;
    Result := True;
  End;
End;

Procedure bAppend(Var Bytes : TBytes; ToAppend : TBytes);

{$IF not defined(DynamicArraySupport)}
Var
  CopyAt                  : Integer;
{$ENDIF}

Begin
{$IF Defined(DynamicArraySupport)}
  Bytes := Bytes+ToAppend;
{$Else}
  CopyAt := Low(Bytes)+Length(Bytes);
  SetLength(Bytes,Length(Bytes)+Length(ToAppend));
  Move(ToAppend[Low(ToAppend)],Bytes[CopyAt],Length(ToAppend));
{$ENDIF}
End;

Function bCopy(Const Bytes : TBytes; Start : Integer; Count : Integer) : TBytes;

{$IF NOT defined(DynamicArraySupport)}
Var
  MaxLen                  : Integer;
{$ENDIF}

Begin
{$IF defined(DynamicArraySupport)}
  Result := Copy(Bytes,Start,Count);
{$Else}
  MaxLen := Length(Bytes)-Start;
  If Count>MaxLen Then Count := MaxLen;
  SetLength(Result,Count);
  Move(Bytes[Start],Result[Low(Result)],Count);
{$ENDIF}
End;

Procedure bDelete(Var Bytes : TBytes; Start : Integer; Count : Integer);

{$IF not defined(DynamicArraySupport)}
Var
  MaxLen                  : Integer;
  Len                     : Integer;
{$ENDIF}

Begin
{$IF defined(DynamicArraySupport)}
  Delete(Bytes,Start,Count);
{$Else}

  Len := Length(Bytes);

  MaxLen := Len-Start;
  If Count>MaxLen Then Count := MaxLen;

  If Count>0 Then
  Begin
    If (Len>Start+Count) Then
    Begin
      Move(Bytes[Start+Count],Bytes[Start], Len-(Start+Count) );
    End;

    SetLength(Bytes,Len-Count);
  End;
{$ENDIF}
End;


{$IFNDEF NEXTGEN}

{$IF defined(Unicode)}
Function bString(Const V : AnsiString; Const StringMode : TStringMode=smUTF8) : TBytes; Overload;

Begin
  Case StringMode of
    smAnsi   : Result := System.SysUtils.TEncoding.Convert(
                             System.SysUtils.TEncoding.Ansi,
                             System.SysUtils.TEncoding.ANSI,
                             System.SysUtils.TEncoding.Ansi.GetBytes(V));
    smUTF8   :   Result := System.SysUtils.TEncoding.Convert(
                             System.SysUtils.TEncoding.Ansi,
                             System.SysUtils.TEncoding.UTF8,
                             System.SysUtils.TEncoding.Ansi.GetBytes(V));
    smUTF16  : Result := System.SysUtils.TEncoding.Convert(
                             System.SysUtils.TEncoding.Ansi,
                             System.SysUtils.TEncoding.Unicode,
                             System.SysUtils.TEncoding.Ansi.GetBytes(V));
  Else
    SetLength(Result,0);
  End;
End;

Function printableBytes(Const S : AnsiString; AllHex : Boolean=False) : String; Overload;

Var
  Loop                    : Integer;

Begin
  Result := '';
  For Loop := 1 To Length(S) Do
  Begin
    If (S[Loop]<#32) Or (S[Loop]>#$7f) Or AllHex Then
    Begin
      Result := Result+Format(' [%.2x] ',[ORd(S[Loop])]);
    End
    Else
    Begin
      Result := Result+ Char(S[Loop]);
    End;
  End;
End;
{$Else}
Function bString(Const V : WideString; Const StringMode : TStringMode=smUTF8) : TBytes;


Var
  U                       : UTF8String;
  A                       : AnsiString;

Begin
  Case StringMode Of
    smRaw,
    smUTF8        : Begin
                      U := UTF8Encode(V);
                      SetLength(Result,Length(U));
                      Move(U[1],Result[Low(Result)],Length(Result));
                    End;
    smAnsi        : Begin
                      A := V;
                      SetLength(Result,Length(A));
                      Move(A[1],Result[Low(Result)],Length(Result));
                    End;
  Else
    SetLength(Result,0);
  End;
End;
Function printableBytes(Const S : WideString; AllHex : Boolean=False) : String;

Var
  Loop                    : Integer;

Begin
  Result := '';
  For Loop := 1 To Length(S) Do
  Begin
    If (S[Loop]<#32) Or (S[Loop]>#$7f) Or AllHex Then
    Begin
      Result := Result+Format(' [%.2x] ',[ORd(S[Loop])]);
    End
    Else
    Begin
      Result := Result+ Char(S[Loop]);
    End;
  End;
End;
{$ENDIF}
{$ENDIF}

{$ENDREGION}



Initialization
  __CRLF   := bString(#13#10,smAnsi);
  __LFCR   := bString(#10#13,smAnsi);
  __CR     := bString(#13,smAnsi);
  __LF     := bString(#10,smAnsi);
end.
