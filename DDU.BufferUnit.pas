unit DDU.BufferUnit;

//*****************************************************************************
//
// DDU.BufferUnit
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
// Purpose : Provides a raw binary buffer that uses linked lists instead
//           of a monolithic memory block for performance issues.
//           Embeded signals are provided for higher level processing
//	         and data tracking
//
// History : <none>
//
//*****************************************************************************

interface

{ $I DVer.inc}

{$undef bufferSTREAMS}

Uses
  System.SysUtils,
  System.Classes;   // TNotifyEvent, TStream


{$I DTypes.inc}

Type
  TStringMode  = (smAnsi{$IF defined(UNICODE)},smUTF8, smRaw{$ENDIF});
  TNodeType    = (ntData, ntSignal {$IF defined(bufferStreams)},ntStream{$ENDIF});
  PNodeDetails = Pointer;

  TSignal=Class
  Private
    fData   : Pointer;
    fDetail : String;
    fID     : Int64;
    fName   : String;
    fObject : TObject;
    fTag    : Cardinal;
  Public
    Procedure Assign(Source : TSignal);
  Public
    Property Write_Data      : Pointer      Read fData               Write fData;
    Property Write_ID        : Int64        Read fID                 Write fID;
    Property Write_Name      : String       Read fName               Write fName;
    Property Write_Detail    : String       Read fDetail             Write fDetail;
    Property Write_Object    : TObject      Read fObject             Write fObject;
    Property Write_Tag       : Cardinal     Read fTag                Write fTag;
  Public
    Property Data      : Pointer      Read fData;
    Property ID        : Int64        Read fID;
    Property Name      : String       Read fName;
    Property Detail    : String       Read fDetail;
    Property &Object   : TObject      Read fObject;
    Property Tag       : Cardinal     Read fTag;
  End;

  TNode = Class
  Private
    fData     : TBytes;
    fDataSize : UInt64;
    fNextNode : TNode;
    fNodeType : TNodeType;
    fPosition : UInt64;
    fPrevNode : TNode;
    fSignal   : TSignal;
{$IF defined(bufferStreams)}
    fStream   : TStream;
{$ENDIF}
    fWriteAt  : UInt64;
  Protected
    Procedure EmptyData;
  Public
    Constructor CreateData(aDataSize : UINT64); Virtual;
{$IF defined(bufferStreams)}
    Constructor CreateStream(aStream : TStream); Virtual;
{$ENDIF}
    Constructor CreateSignal(aSignal : TSignal); Virtual;
    Destructor Destroy; Override;
    Procedure Clear; Virtual;
    function AddData(Var aData : PByte; Var AmountToWrite : UInt64) : UInt64;
    Function AddBytes(B : TBytes; Var WriteFrom : UInt64; Var AmountToWrite : UInt64) : UInt64;
  Public
    Property NextNode : TNode        Read fNextNode Write fNextNode;
    Property PrevNode : TNode        Read fPrevNode Write fPrevNode;
    Property NodeType : TNodeType    Read fNodeType;
  // ntData
    Property Data     : TBytes Read fData     Write fData;
    Property DataSize : UInt64 Read fDataSize Write fDataSize;
    Property WriteAt  : UInt64 Read fWriteAt  Write fWriteAt;
    Property Position : UInt64 Read fPosition Write fPosition;
  // ntSignal
    Property aSignal  : TSignal     Read fSignal;
{$IF defined(bufferStreams)}
  // ntStream
    Property Stream   : TStream     Read fStream Write fStream;
{$ENDIF}
  End;

  TNotifyEvent=Procedure(Sender : TObject) Of Object;

  TDDUBuffer=Class
  Private
    fDataNodeSize    : UINT64;
    fFindBufferSize  : UINT64;
    fFirstNode       : TNode;
    fLastNode        : TNode;
    fNodeCount       : UINT64;
    fOnChange        : TNotifyEvent;
    fSize            : UINT64;
    fStringWriteMode : TStringMode;
    fStringReadMode  : TStringMode;
    fSignal          : TSignal;
    fSignaled        : Boolean;
    fThreadSafe      : Boolean;

    function GetAvailable     : UINT64;
    function GetDataAvailable : UINT64;
    function GetEmpty         : Boolean;

    procedure SetDataNodeSize(const Value : UINT64);
    procedure SetFindBufferSize(const Value: UINT64);
    procedure SetFirstNode(const Value: TNode);
    procedure SetThreadSafe(const Value: Boolean);

    Property FirstNode : TNode Read fFirstNode Write SetFirstNode;
    Property LastNode  : TNode Read fLastNode  Write fLastNode;
  Protected
    Procedure ReleaseNode(Var aNode : TNode); Virtual;
    Procedure ReleaseFirstNode; Virtual;
    Procedure ReleaseLastNode; Virtual;

    Function AddNode(aNode : TNode) : TNode; Virtual;
    Procedure DoChange; Virtual;

    Function Internal_ReadData(Data : Pointer; TotalReadSize : UINT64; Peeking : Boolean) : UINT64; Virtual;
    Function Internal_ReadBytes(TotalReadSize : UINT64; Peeking : Boolean) : TBytes; Virtual;
  Public
    Constructor Create; Virtual;
    Destructor Destroy; Override;

    Procedure Lock; Inline;
    Procedure Unlock; Inline;
  Public
  Const
    DefaultDataNodeSize = 4*1024;
    DefaultFindBufferSize = 64*1024;
  Public
    // Data access primitives

    Procedure ClearSignal; Virtual;
    function  FindData(Data : TBytes; Out Position : UInt64) : Boolean; Virtual;
    Procedure Flush; Virtual;

    Function  PeekData(Data : Pointer; PeekSize : UINT64) : UINT64; Virtual;
    Function  ReadData(Data : Pointer; ReadSize : UINT64) : UINT64; Virtual;
    Function  Seek(SeekSize : UINT64) : UINT64; Virtual;

    Procedure WriteData(Data : Pointer; Size : UINT64); Virtual;
    Procedure WriteSignal(aID : Int64; Const aName : String=''; Const aDetail : String=''; aTag : Integer=0; anObject : TObject=Nil; aData : Pointer=Nil); Virtual;
{$IF defined(bufferStreams)}
    Procedure WriteStream(S : TStream; TakeOwnership : Boolean=True ); Virtual;
{$ENDIF}
    Function  UnwriteData(TotalToUnwrite: UInt64) : UInt64; Virtual;
    Procedure TossFirstNode; Virtual;
    Procedure StealNodes(Source : TDDUBuffer); Virtual;
  Public
    // Advanced read/write functions
    Function  PeekBytes(PeekSize : UINT64=0) : TBytes; Overload;
    Function  ReadBytes(ReadSize : UInt64=0) : TBytes; Overload;
    Procedure WriteByte(Const B : Byte);
    Procedure WriteBytes(Bytes : TBytes);
    Procedure WriteString(Const S : String); Overload;
    Procedure WriteString(Const S: String; StringMode : TStringMode); Overload;
    Function  PeekString(PeekSize : UINT64=0) : String; Overload;
    Function  PeekString(StringMode : TStringMode; PeekSize : UINT64=0) : String; Overload;
    Function  ReadLine : String; Overload;
    Function  ReadLine(StringMode : TStringMode) : String; Overload;
    function  ReadString(ReadSize : UINT64=0) : String; Overload;
    function  ReadString(StringMode : TStringMode; ReadSize : UINT64=0)  : String; Overload;
  Public
    Property Available       : UINT64       Read GetAvailable;     // Available is everything before the end or next signal
    Property DataAvailable   : UINT64       Read GetDataAvailable; // Available is everything before the end or next signal or stream
    Property DataNodeSize    : UINT64       Read fDataNodeSize     Write SetDataNodeSize   Default DefaultDataNodeSize;
    Property Empty           : Boolean      Read GetEmpty;
    Property FindBufferSize  : UINT64       Read fFindBufferSize   Write SetFindBufferSize Default DefaultFindBufferSize;
    Property NodeCount       : UINT64       Read fNodeCount;
    Property OnChange        : TNotifyEvent read fOnChange Write fOnChange;

    Property Signaled        : Boolean      Read fSignaled;
    Property Signal          : TSignal      Read fSignal;

    Property Size            : UINT64       Read fSize;                                                                    // Sum of all data and stream nodes available
    Property StringWriteMode : TStringMode  Read fStringWriteMode  Write fStringWriteMode;
    Property StringReadMode  : TStringMode  Read fStringReadMode   Write fStringReadMode;

    Property ThreadSafe      : Boolean      Read fThreadSafe       Write SetThreadSafe; // not yet implemented
  End;

  EBufferSignaled=Class(Exception) End;

Function Bytes(Const V : Array Of Byte) : TBytes;

Type
  // Provides a TStream interface to a TDDUBuffer object (either created automatically or
  // provided by the creator).
  //
  // This abstracts the TDDUBuffer mechanism to something more generic
  //
  TDDUBufferStream=Class(TStream)
  Private
    fBuffer     : TDDUBuffer;
    fFreeBuffer : Boolean;
    fOnWrite    : TNotifyEvent;
  Protected
    function GetSize: Int64; Override;
  Public
    Constructor Create; Overload; Virtual;
    Constructor Create(aBuffer : TDDUBuffer; FreeOnDestroy: Boolean=False); Overload; Virtual;
    Destructor Destroy; Override;

    function Read(var Buffer; Count: Longint): Longint; Override;
    function Write(const Buffer; Count: Longint): Longint; Override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; Override;

    Property OnWrite : TNotifyEvent Read fOnWrite Write fOnWrite;
  End;

implementation

Function Min(A,B : UInt64) : UInt64; inline;

Begin
  If A<B Then Result := A Else Result := B;
End;

Function Bytes(Const V : Array Of Byte) : TBytes;

Var
  Loop                    : Integer;

Begin
  SetLength(Result,High(V)-Low(V));
  For Loop := Low(V) TO High(V) Do
  Begin
    Result[Low(Result)+Loop-Low(V)] := V[Loop];
  End;
End;

{ TSignal }

procedure TSignal.Assign(Source: TSignal);
begin
  fData   := Source.fData;
  fDetail := Source.fDetail;
  fID     := Source.fID;
  fName   := Source.fName;
  fObject := Source.fObject;
  fTag    := Source.fTag;
end;

{$REGION 'TNode'}

{ TNode }

function TNode.AddBytes(B: TBytes; var WriteFrom, AmountToWrite: UInt64): UInt64;
begin
// Calculate how much can be writen into the node
  Result := Min(DataSize-WriteAt,AmountToWrite);
// Copy that much data in.
  Move(B[WriteFrom], Data[WriteAt],Result);
  Inc(fWriteAt,Result);
  Inc(Writefrom,Result);
  Dec(AmountToWrite,Result);
end;

function TNode.AddData(Var aData : PByte; Var AmountToWrite : UInt64) : UInt64;
begin
// Calculate how much can be writen into the node
  Result := Min(DataSize-WriteAt,AmountToWrite);
// Copy that much data in.
  Move(aData^, Data[WriteAt], Result);
  Inc(fWriteAt,Result);
// Move pointer forward.
  Inc(aData,Result);
// Decrease amount to write
  AmountToWrite := AmountToWrite-Result;
end;

procedure TNode.Clear;
begin
  SetLength(fData,0);
  fDataSize := 0;

  FreeAndNil(fSignal);
{$IF defined(bufferStreams)}
  FreeAndNil(fStream);
{$ENDIF}
end;

constructor TNode.CreateData(aDataSize: UINT64);
begin
  Inherited Create;
  fNodeType := ntData;
  fDataSize := aDataSize;
  SetLength(fData,aDataSize);
end;

constructor TNode.CreateSignal(aSignal: TSignal);
begin
  Inherited Create;
  fNodeType := ntSignal;
  fSignal := aSignal;
end;

{$IF defined(bufferStreams)}
constructor TNode.CreateStream(aStream: TStream);
begin
  Inherited Create;
  fNodeType := ntStream;
  fStream := aStream;
end;
{$ENDIF}

destructor TNode.Destroy;
begin
  Clear;
  inherited;
end;

procedure TNode.EmptyData;
begin
  fWriteAt  := 0;
  fPosition := 0;
end;

{$ENDREGION}

{$REGION 'TDDUBuffer'}

{ TDDUBuffer }

Function TDDUBuffer.AddNode(aNode : TNode) : TNode;

begin
  Result := aNode;
  If Assigned(FirstNode) And (FirstNode.NodeType=ntData) And (FirstNode.WriteAt=0) Then
  Begin
    ReleaseFirstNode;
  End;

  aNode.NextNode := Nil;
  aNode.PrevNode := LastNode; // create backward link

  If Assigned(LastNode) Then
  Begin
    LastNode.NextNode := aNode;  // create forward link
    LastNode := aNode;
  End
  Else
  Begin
    FirstNode := aNode;
    LastNode  := aNode;
  End;

{$IF defined(bufferStreams)}
  If aNode.NodeType=ntStream Then
  Begin
    fSize := fSize+ (aNode.Stream.Size-aNode.Stream.Position);
  End;
{$ENDIF}
  Inc(fNodeCount);
end;

procedure TDDUBuffer.ClearSignal;
begin
  If Signaled Then
  Begin
    ReleaseFirstNode;
  End;
  DoChange;
end;

constructor TDDUBuffer.Create;
begin
  Inherited Create;
//  fSignal := TSignal.Create;
//  fStringWriteMode := smAnsi;
//  fStringReadMode  := smAnsi;
  fStringWriteMode := smUTF8;
  fStringReadMode  := smUTF8;
  DataNodeSize     := DefaultDataNodeSize;
end;

destructor TDDUBuffer.Destroy;
begin
  Flush;
//  fSignal.Free;
  inherited;
end;

procedure TDDUBuffer.DoChange;
begin
  If Assigned(fOnChange) Then
  Begin
    fOnChange(Self);
  End;
end;

function TDDUBuffer.FindData(Data : TBytes; Out Position : UInt64) : Boolean;

Var
  AbsoluteOffset          : Int64;
  aNode                   : TNode;
  At                      : Integer;
  MatchBuffer             : TBytes;
  MatchCount              : Integer;
  MatchLen                : Integer;
{$IF defined(bufferStreams)}
  OriginalPosition        : Int64;
  Stream                  : TStream;
  StreamBuffer            : TBytes;
  ToRead                  : Int64;
{$ENDIF}

Function CheckChar(C : Byte) : Boolean;

Var
  P,P1 : PByte;

Begin
// Add the character into the matchbuffer,

  If (MatchLen>1) Then
  Begin
    P  := PByte(Pointer(MatchBuffer));
    P1 := Pointer(NativeInt(P)+1);
    Move(P1^,P^,MatchLen-1);
  End;

  If (MatchCount<MatchLen) Then
  Begin
    Inc(MatchCount);
  End;
  MatchBuffer[MatchLen-1] := C;
  Inc(AbsoluteOffset);

  Result := (MatchCount=MatchLen) And CompareMem(@MatchBuffer[Low(MatchBuffer)],
                                                 @Data[Low(Data)],
                                                 MatchLen);
End;

begin
  Result         := False;
  Position       := 0;
  AbsoluteOffset := 0;

  If Assigned(FirstNode) And (Length(Data)<>0) Then
  Begin
    MatchLen  := Length(Data);
    SetLength(MatchBuffer,MatchLen);
    FillChar(MatchBuffer[Low(MatchBuffer)],Length(MatchBuffer),0);
    MatchCount := 0;

    aNode := FirstNode;
    While (Not Result) And Assigned(aNode) And (aNode.NodeType<>ntSignal) Do
    Begin
      Case aNode.NodeType Of
        ntData    : Begin
                      At          := aNode.Position;

                      While (At<aNode.WriteAt) Do
                      Begin
                        If CheckChar(aNode.Data[At]) Then
                        Begin
                          Position := AbsoluteOffset-MatchLen;//+1;
                          Result := True;
                          Break;
                        End;
                        Inc(At);
                      End;
                    End;
{$IF defined(bufferStreams)}
        ntStream  : Begin
                      Stream           := aNode.Stream;
                      OriginalPosition := Stream.Position;

                      SetLength(StreamBuffer,DefaultFindBufferSize);
                      Try
                        While (Not Result) And (Stream.Position<Stream.Size) Do
                        Begin
                          ToRead := Stream.Size-Stream.Position;
                          If ToRead>DefaultFindBufferSize Then
                          Begin
                            ToRead := DefaultFindBufferSize;
                          End;

                          Stream.Read(StreamBuffer[Low(StreamBuffer)],ToRead);

                          For At := Low(StreamBuffer) To Low(StreamBuffer)+ToRead-1 Do
                          Begin
                            If CheckChar(StreamBuffer[At]) Then
                            Begin
                              Position := AbsoluteOffset-MatchLen+1;
                              Result   := True;
                              Break;
                            End;
                          End;
                        End;
                      Finally
                        Stream.Position := OriginalPosition;
                      End;
                    End;
{$ENDIF}
      End;
      aNode := aNode.NextNode;
    End;
  End;
end;

procedure TDDUBuffer.Flush;
begin
  Lock;
  Try
    While Assigned(FirstNode) Do
    Begin
      ReleaseFirstNode;
    End;
  Finally
    Unlock;
  End;
end;

function TDDUBuffer.GetAvailable: UINT64;

Var
  aNode                   : TNode;
  DataToRead              : UINT64;

begin
  Lock;
  Try
    If Assigned(FirstNode) Then
    Begin
      aNode := FirstNode;
      Result := 0;
      While Assigned(aNode) And (aNode.NodeType<>ntSignal) Do
      Begin
        Case aNode.NodeType Of
          ntData   : Begin
                       DataToRead := aNode.WriteAt-aNode.Position;
                       Inc(Result,DataToRead);
                       aNode := aNode.NextNode;
                     End;
{$IF defined(bufferStreams)}
          ntStream : Begin
                       DataToRead := aNode.Stream.Size-aNode.Stream.Position;
                       Inc(Result,DataToRead);
                       aNode := aNode.NextNode;
                     End;
{$ENDIF}
        End;
      End;
    End
    Else
    Begin
      Result := 0;
    End;
  Finally
    Unlock;
  End;
end;

function TDDUBuffer.GetDataAvailable: UINT64;

Var
  aNode                   : TNode;
  DataToRead              : UINT64;

begin
  Lock;
  Try
    If Assigned(FirstNode) Then
    Begin
      aNode := FirstNode;
      Result := 0;
      While Assigned(aNode) And (aNode.NodeType=ntData) Do
      Begin
        DataToRead := aNode.WriteAt-aNode.Position;
        Inc(Result,DataToRead);
        aNode := aNode.NextNode;
      End;
    End
    Else
    Begin
      Result := 0;
    End;
  Finally
    Unlock;
  End;
end;

function TDDUBuffer.GetEmpty: Boolean;
begin
  Lock;
  Try
    If Assigned(fFirstNode) Then
    Begin
      // If we have nodes, the only way we can be empty is if there is *NO* data
      // written in the first node - anything else means any other data.
      Result := (FirstNode=LastNode) And (FirstNode.NodeType=ntData) And (FirstNode.WriteAt=0)
    End
    ELse
    Begin
      Result := True;
    End;
  Finally
    Unlock;
  End;
end;

// If Data=Nil, then we are seeking instead of reading.  Peeking keeps the read pointers from moving.
// seeking and peeking would be utterly pointless as no data would be read and no pointers would change.
function TDDUBuffer.Internal_ReadBytes(TotalReadSize: UINT64; Peeking: Boolean): TBytes;

Var
  DataToRead              : UINT64;
  DataNeeded              : UINT64;
{$IF defined(bufferStreams)}
  Stream                  : TStream;
  OriginalStreamPosition  : Int64;
{$ENDIF}
  aNode                   : TNode;
  CopyTo                  : UInt64;

begin
  SetLength(Result,TotalReadSize);
  CopyTo := 0;

  Lock;
  Try
    If Assigned(FirstNode) And (TotalReadSize<>0) Then
    Begin
      If (FirstNode.NodeType=ntSignal) Then
      Begin
        Raise EBufferSignaled.Create('Handle Buffer Signal first.');
      End;

//      OriginalStreamPosition := 0;
      aNode                  := FirstNode;
      While (CopyTo<TotalReadSize) And Assigned(aNode) And (aNode.NodeType<>ntSignal) Do
      Begin
        DataNeeded := (TotalReadSize-CopyTo);
        Case aNode.NodeType Of
          ntData   : Begin
                       DataToRead  := Min(aNode.WriteAt-aNode.Position, DataNeeded);

                       // Copy Data
                       Move(aNode.Data[aNode.Position],Result[CopyTo],DataToRead);
                       If (Not Peeking) Then
                       Begin
                         aNode.Position := aNode.Position+DataToRead;
                       End;

                       // Record move
                       Inc(CopyTo,DataToRead);
                       If (Not Peeking) Then
                       Begin
                         Dec(fSize,DataToRead);
                       End;

                       // Is the block finished?
                       If (Not Peeking) And (aNode.Position>=aNode.WriteAt) Then
                       Begin
                         // If the only node in the list is a data node
                         // and it is empty, just reset it instead of freeing it.
                         // That way we can reuse the node if we write more
                         // data.  If we add any other type of node, we can discard it then.
                         If (FirstNode=LastNode) Then
                         Begin
                           aNode.WriteAt  := 0;
                           aNode.Position := 0;
                         End
                         Else
                         Begin
                           ReleaseFirstNode;
                         End;
                       End;
                     End;
{$IF defined(bufferStreams)}
          ntStream : Begin
                       Stream        := aNode.Stream;
                       DataToRead    := Min(Stream.Size-Stream.Position,DataNeeded);

                       // Copy Data
                       OriginalStreamPosition := Stream.Position;
{ TODO : We do not seem to account for read problems here.  oversight? }
                       aNode.Stream.ReadBuffer(Result[CopyTo],DataToRead);
                       If Peeking Then
                       Begin
                         Stream.Position := OriginalStreamPosition;
                       End;

                       // Record Move
                       Inc(CopyTo,DataToRead);
                       If (Not Peeking) Then
                       Begin
                         Dec(fSize,DataToRead);
                       End;

                       // Is the block finished?
                       If (Not Peeking) And (aNode.Stream.Position>=aNode.Stream.Size) Then
                       Begin
                         ReleaseFirstNode;
                       End;
                     End;
{$ENDIF}
        End;
        If Peeking Then
        Begin
          aNode := aNode.NextNode;
        End
        Else
        Begin
          aNode := FirstNode;
        End;
      End;
    End;
    If (Not Peeking) Then
    Begin
      DoChange;
    End;
  Finally
    SetLength(Result,CopyTo);
    Unlock;
  End;
End;

function TDDUBuffer.Internal_ReadData(Data : Pointer; TotalReadSize : UINT64; Peeking : Boolean) : UINT64;

Var
  DataToRead              : UINT64;
  DataNeeded              : UINT64;
{$IF defined(bufferStreams)}
  Stream                  : TStream;
  OriginalStreamPosition  : Int64;
{$ENDIF}
  aNode                   : TNode;

begin
  Lock;
  Try
    Result := 0;
    If Assigned(FirstNode) And (TotalReadSize<>0) Then
    Begin
      If (FirstNode.NodeType=ntSignal) Then
      Begin
        Raise EBufferSignaled.Create('Handle Buffer Signal first.');
      End;

      aNode                  := FirstNode;
      While (Result<TotalReadSize) And Assigned(aNode) And (aNode.NodeType<>ntSignal) Do
      Begin
        DataNeeded := (TotalReadSize-Result);
        Case aNode.NodeType Of
          ntData   : Begin
                       DataToRead  := Min(aNode.WriteAt-aNode.Position, DataNeeded);

                       // Copy Data
                       If Assigned(Data) Then
                       Begin
                         Move(aNode.Data[aNode.Position],PByte(Data)[Result],DataToRead);
                       End;
                       If (Not Peeking) Then
                       Begin
                         aNode.Position := aNode.Position+DataToRead;
                       End;

                       // Record move
                       Inc(Result,DataToRead);
                       If (Not Peeking) Then
                       Begin
                         Dec(fSize,DataToRead);
                       End;

                       // Is the block finished?
                       If (Not Peeking) And (aNode.Position>=aNode.WriteAt) Then
                       Begin
                         // If the only node in the list is a data node
                         // and it is empty, just reset it instead of freeing it.
                         // That way we can reuse the node if we write more
                         // data.  If we add any other type of node, we can discard it then.
                         If (FirstNode=LastNode) Then
                         Begin
                           aNode.WriteAt  := 0;
                           aNode.Position := 0;
                         End
                         Else
                         Begin
                           ReleaseFirstNode;
                         End;
                       End;
                     End;
{$IF defined(bufferStreams)}
          ntStream : Begin
                       Stream        := aNode.Stream;
                       DataToRead    := Min(Stream.Size-Stream.Position,DataNeeded);

                       // Copy Data
                       If Assigned(Data) Then // Reading or peeking
                       Begin
                         OriginalStreamPosition := Stream.Position;
                         aNode.Stream.ReadBuffer(PByte(Data)[Result],DataToRead);
                         If Peeking Then
                         Begin
                           Stream.Position := OriginalStreamPosition;
                         End;
                       End
                       Else
                       Begin // Seeking
                         If (Not Peeking) Then
                         Begin
                           aNode.Stream.Position := aNode.Stream.Position+DataToRead;
                         End;
                       End;

                       // Record Move
                       Inc(Result,DataToRead);
                       If (Not Peeking) Then
                       Begin
                         Dec(fSize,DataToRead);
                       End;

                       // Is the block finished?
                       If (Not Peeking) And (aNode.Stream.Position>=aNode.Stream.Size) Then
                       Begin
                         ReleaseFirstNode;
                       End;
                     End;
{$ENDIF}
        End;
        If Peeking Then
        Begin
          aNode := aNode.NextNode;
        End
        Else
        Begin
          aNode := FirstNode;
        End;
      End;
    End;
    If (Not Peeking) Then
    Begin
      DoChange;
    End;
  Finally
    Unlock;
  End;
End;

procedure TDDUBuffer.Lock;
begin
  If ThreadSafe Then
    TDDUMonitor.Enter(Self.ClassName,Self);
end;

function TDDUBuffer.PeekBytes(PeekSize: UINT64): TBytes;

Var
  lAvailable              : UINT64;

begin
  lAvailable := Available;
  If (PeekSize>lAvailable) Then
  Begin
    PeekSize := lAvailable;
  End;

  Result := Internal_ReadBytes(PeekSize,True);
end;

function TDDUBuffer.PeekData(Data: Pointer; PeekSize: UINT64): UINT64;

begin
  Result := Internal_ReadData(Data,PeekSize,True);
end;

function TDDUBuffer.PeekString(PeekSize: UINT64): String;
begin
  Result := PeekString(StringReadMode,PeekSize);
end;

function TDDUBuffer.PeekString(StringMode: TStringMode; PeekSize: UINT64): String;

Var
  B                       : TBytes;

begin
  B := PeekBytes(PeekSize);
{$If defined(UNICODE)}
  Case StringMode Of
    smRaw  : Result := TEncoding.ASCII.GetString(B);
    smAnsi : Result := TEncoding.ANSI.GetString(B);
    smUTF8 : Result := TEncoding.UTF8.GetString(B);
  End;
{$ELSE}
  SetLength(result,Length(B));
  Move(B[Low(B)],Result[Low(Result)],Length(B));
{$ENDIF}
end;

function TDDUBuffer.ReadBytes(ReadSize: UInt64): TBytes;
Var
  fAvailable              : UINT64;

begin
  fAvailable := Available;
  If (ReadSize>fAvailable) Then
  Begin
    ReadSize := fAvailable;
  End;
  Result := Internal_ReadBytes(ReadSize,False);
end;

function TDDUBuffer.ReadData(Data: Pointer; ReadSize: UINT64): UINT64;

Begin
  Result := Internal_ReadData(Data,ReadSize,False);
End;

function TDDUBuffer.ReadLine: String;
begin
  Result := ReadLine(StringReadMode);
end;

function TDDUBuffer.ReadLine(StringMode: TStringMode): String;

Var
  At                      : UInt64;

begin
  If FindData(Bytes([13,10]),At) Then
  Begin
    If At>1 Then
    Begin
      Result := ReadString(StringMode,At-1);
    End
    Else
    Begin
      Result := '';
    End;
    Seek(2);
  End
  Else
  Begin
    Result := ReadString(StringMode,Available);
  End;
end;

function TDDUBuffer.ReadString(ReadSize: UINT64): String;
begin
  Result := ReadString(StringReadMode,ReadSize);
end;

function TDDUBuffer.ReadString(StringMode: TStringMode; ReadSize: UINT64): String;

Var
  B                       : TBytes;

begin
  B := ReadBytes(ReadSize);
{$If defined(UNICODE)}
  Case StringMode Of
    smRaw  : Result := TEncoding.ASCII.GetString(B);
    smAnsi : Result := TEncoding.ANSI.GetString(B);
    smUTF8 : Result := TEncoding.UTF8.GetString(B);
  End;
{$ELSE}
  SetLength(result,Length(B));
  Move(B[Low(B)],Result[Low(Result)],Length(B));
{$ENDIF}
end;

procedure TDDUBuffer.ReleaseFirstNode;

Var
  aNode                   : TNode;

begin
  If Assigned(FirstNode) Then
  Begin
    aNode     := FirstNode;
    FirstNode := aNode.NextNode;

    If Assigned(FirstNode) Then
    Begin
      FirstNode.PrevNode := Nil;
    End
    Else
    Begin
      LastNode := Nil;
    End;

    ReleaseNode(aNode);
  End;
end;

procedure TDDUBuffer.ReleaseLastNode;

Var
  aNode                   : TNode;

begin
  If Assigned(LastNode) Then
  Begin
    aNode      := LastNode;
    LastNode  := aNode.PrevNode;

    If Assigned(LastNode) Then
    Begin
      LastNode.NextNode := Nil;
    End
    Else
    Begin
      FirstNode := Nil;
    End;

    ReleaseNode(aNode);
  End;
end;

procedure TDDUBuffer.ReleaseNode(Var aNode: TNode);

begin
  Case aNode.NodeType Of
    ntData             : Begin
                           If (aNode.Position<aNode.WriteAt) Then
                           Begin
                             fSize := fSize-(aNode.WriteAt-aNode.Position);
                           End;
                         End;
{$IF defined(bufferStreams)}
    ntStream           : Begin
                           If aNode.Stream.Position <aNode.Stream.Size Then
                           Begin
                             fSize := fSize-(aNode.Stream.Size-aNode.Stream.Position);
                           End;
                         End;
{$ENDIF}
  End;
  aNode.Clear;
  aNode.Free;
  aNode := Nil;

  Dec(fNodeCount);
end;

function TDDUBuffer.Seek(SeekSize: UINT64): UINT64;

begin
  Result := Internal_ReadData(Nil,SeekSize,False);
End;

procedure TDDUBuffer.SetDataNodeSize(const Value: UINT64);
begin
  If Value=0 Then
  Begin
    fDataNodeSize := DefaultDataNodeSize;
  End
  Else
  Begin
    fDataNodeSize := Value;
  End;
end;

procedure TDDUBuffer.SetFindBufferSize(const Value: UINT64);
begin
  If Value=0 Then
  Begin
    fFindBufferSize := DefaultFindBufferSize;
  End
  Else
  Begin
    fFindBufferSize := Value;
  End;
end;

procedure TDDUBuffer.SetFirstNode(const Value: TNode);
begin
  fFirstNode := Value;
  fSignaled := Assigned(FirstNode) And (FirstNode.NodeType = ntSignal);
  If Signaled Then
  Begin
    fSignal := FirstNode.aSignal;
  End
  Else
  Begin
    fSignal := Nil;
  End;
  DoChange;
end;

procedure TDDUBuffer.SetThreadSafe(const Value: Boolean);
begin
  TMonitor.Enter(Self); // This ensures we have a monitor already ready.
  fThreadSafe := Value;
  TMonitor.Exit(Self);
end;

procedure TDDUBuffer.StealNodes(Source: TDDUBuffer);
begin
  Lock;
  Try
    Source.Lock;
    Try
      If Assigned(Source.FirstNode) Then
      Begin
        If Assigned(LastNode) Then
        Begin
          LastNode.NextNode := Source.FirstNode;
          Source.FirstNode.PrevNode := LastNode;
        End
        Else
        Begin
          FirstNode := Source.FirstNode;
        End;
        LastNode  := Source.LastNode;
        fSize      := fSize+Source.fSize;
        fNodeCount := fNodeCount+Source.fNodeCount;

        Source.fSize      := 0;
        Source.fNodeCount := 0;
        Source.FirstNode := Nil;
        Source.LastNode  := Nil;
      End;
    Finally
      Source.Unlock;
    End;
  Finally
    Unlock;
  End;
end;

procedure TDDUBuffer.TossFirstNode;

begin
  Lock;
  Try
    If Assigned(FirstNode) Then
    Begin
      If (FirstNode=LastNode) And (FirstNode.NodeType=ntData) Then
      Begin
        FirstNode.EmptyData;
        fSize := 0;
      End
      Else
      Begin
        ReleaseFirstNode;
      End;
    End;
  Finally
    Unlock;
  End;
end;

procedure TDDUBuffer.Unlock;
begin
  If ThreadSafe Then
    TDDUMonitor.Exit(Self.ClassName,Self);
end;

function TDDUBuffer.UnwriteData(TotalToUnwrite: UInt64): UInt64;

Var
  aNode                   : TNode;
  DataToRemove            : UInt64;

begin
  Lock;
  Try
    Result := 0;

    aNode  := LastNode;
    While Assigned(aNode) And (aNode.NodeType=ntData) And (Result<TotalToUnwrite) Do
    Begin

      DataToRemove := aNode.WriteAt;
      If DataToRemove>(TotalToUnwrite-Result) Then
      Begin
        DataToRemove := (TotalToUnwrite-Result);
      End;

      aNode.WriteAt := aNode.WriteAt-DataToRemove;
      Result := Result+DataToRemove;
      fSize := fSize-DataToRemove;

      If (aNode.WriteAt=0) Then
      Begin
        If (aNode=FirstNode) Then
        Begin
          Break;
        End
        Else
        Begin
          ReleaseLastNode;
        End;
      End;
      aNode := LastNode;
    End;
    DoChange;
  Finally
    Unlock;
  End;
end;

procedure TDDUBuffer.WriteByte(const B: Byte);
begin
  WriteData(Pointer(@B),1);
end;

procedure TDDUBuffer.WriteBytes(Bytes: TBytes);

Var
  WriteFrom               : UInt64;
  WriteSize               : UInt64;
  NewNode                 : TNode;

begin
  Lock;
  Try
    WriteFrom := 0;
    WriteSize := Length(Bytes);

    If (WriteSize=0) Then
    Begin
      Exit;
    End;

    If Assigned(LastNode) And (LastNode.NodeType=ntData) Then
    Begin
      fSize := fSize+LastNode.AddBytes(Bytes,WriteFrom,WriteSize);
    End;

    While (WriteSize>0) Do
    Begin
      NewNode := AddNode(TNode.CreateData(DataNodeSize));
      fSize := fSize+ NewNode.AddBytes(Bytes,WriteFrom,WriteSize);
    End;
    DoChange;
  Finally
    Unlock;
  End;
end;

procedure TDDUBuffer.WriteData(Data: Pointer; Size: UINT64);

Var
  NewNode                 : TNode;

begin
  Lock;
  Try
    If (Data=Nil) Then
    Begin
      Exit;
//      Raise Exception.Create('Can not write NIL data');
    End;

    If Assigned(LastNode) And (LastNode.NodeType=ntData) Then
    Begin
      fSize := fSize+LastNode.AddData(PByte(Data), Size);
    End;

    While (Size>0) Do
    Begin
      NewNode := AddNode(TNode.CreateData(DataNodeSize));
      fSize := fSize+ NewNode.AddData(PByte(Data),Size);
    End;
    DoChange;
  Finally
    Unlock;
  End;
end;

procedure TDDUBuffer.WriteSignal(aID : Int64; Const aName : String=''; Const aDetail : String=''; aTag : Integer=0; anObject : TObject=Nil; aData : Pointer=Nil);

Var
  aSignal              : TSignal;

begin
  Lock;
  Try
    aSignal := TSignal.Create;

    aSignal.fID       := aID;
    aSignal.fName     := aName;
    aSignal.fDetail   := aDetail;
    aSignal.fTag      := aTag;
    aSignal.fObject   := anObject;
    aSignal.fData     := aData;

    AddNode(TNode.CreateSignal(aSignal));
    DoChange;
  Finally
    Unlock;
  End;
end;

{$IF defined(bufferStreams)}
procedure TDDUBuffer.WriteStream(S : TStream; TakeOWnership : Boolean=True);

Var
  aSize                  : Int64;
  Buffer                 : TBytes;

begin
  Try
    If S=Nil Then
    Begin
      Raise Exception.Create('Can not write NIL stream');
    End;

    If TakeOwnership Then
    Begin
      AddNode(TNode.CreateStream(S));
      DoChange;
    End
    Else
    Begin
      Repeat
        aSize := S.Size-S.Position;
        If aSize>DataNodeSize Then
        Begin
          aSize := DataNodeSize;
        End;

        If (aSize<>0) Then
        Begin
          SetLength(Buffer,aSize);
          S.Read(Buffer[Low(Buffer)],aSize);
          WriteBytes(Buffer);
        End;
      Until (aSize=0);
    End;
  Finally
    Unlock;
  End;
end;
{$ENDIF}

procedure TDDUBuffer.WriteString(Const S: String; StringMode: TStringMode);

Var
  B                       : TBytes;
  Encoding                : TEncoding;

Begin
  If S='' Then Exit;
  Case StringMode Of
    smAnsi : Encoding := TEncoding.ANSI;
    smUTF8 : Encoding := TEncoding.UTF8;
  Else
    Encoding := TEncoding.ASCII;
  End;
  B := System.SysUtils.TEncoding.Convert( System.SysUtils.TEncoding.Unicode, Encoding,
            System.SysUtils.TEncoding.Unicode.GetBytes(S));
  WriteBytes(B);
End;

procedure TDDUBuffer.WriteString(Const S: String);

Begin
  WriteString(S,StringWriteMode);
End;

{$ENDREGION}

{$REGION 'TDDUBufferStream'}

{ TDDUBufferStream }

constructor TDDUBufferStream.Create;
begin
  Inherited Create;
  fBuffer     := TDDUBuffer.Create;
  fFreeBuffer := True;
end;

constructor TDDUBufferStream.Create(aBuffer: TDDUBuffer; FreeOnDestroy: Boolean);
begin
  Inherited Create;
  fBuffer     := aBuffer;
  fFreeBuffer := FreeOnDestroy;
end;

destructor TDDUBufferStream.Destroy;
begin
  If fFreeBuffer Then
  Begin
    fBuffer.Free;
  End;
  inherited;
end;

function TDDUBufferStream.GetSize: Int64;
begin
  Result := fBuffer.Size;
end;

function TDDUBufferStream.Read(var Buffer; Count: Integer): Longint;
begin
  Result := Count;
  If Result>fBuffer.DataAvailable Then Result := fBuffer.DataAvailable;
  fBuffer.ReadData(@Buffer,Result);
end;

function TDDUBufferStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  Case Origin Of
    soBeginning : Begin End;
    soCurrent   : fBuffer.Seek(Offset);
    soEnd       : fBuffer.Flush;
  End;
  Result := 0;
end;

function TDDUBufferStream.Write(const Buffer; Count: Integer): Longint;
begin
  fBuffer.WriteData(@Buffer,Count);
  Result := Count;
  If Assigned(fOnWrite) Then
  Begin
    fOnWrite(Self);
  End;
end;

{$ENDREGION}

end.


