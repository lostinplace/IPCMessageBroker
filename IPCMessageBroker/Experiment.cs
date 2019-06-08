using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;
using ZeroFormatter;
using IPCMessageBroker.ConsumerRecordTracking;

namespace IPCMessageBroker
{

  

  public enum RecipientType : byte
  {
    Any = 0,
    Client = 1,
    Server = 2,
  }


  public enum OperationType : byte
  {
    Destroy = 0,
    Create = 1,
    Update=2
  }

  [ZeroFormattable]
  public struct QueueMessage
  {
    public QueueMessage(RecipientType recipient, ulong payload, uint index, uint payloadSize, OperationType messageType)
    {
      Recipient = recipient;
      Payload = payload;
      this.index = index;
      PayloadSize = payloadSize;
      OperationType = messageType;
    }

    [Index(0)]
    public RecipientType Recipient { get; set; }

    [Index(1)]
    public ulong Payload { get; set; }

    [Index(2)]
    public uint index { get; set; } 

    [Index(3)]
    public uint PayloadSize { get; set; }

    [Index(4)]
    public OperationType OperationType { get; set; }

  }


  [Serializable]
  public class Vector3
  {
    public double x { get; set; }
    public double y { get; set; }
    public double z { get; set; }

  }

  [Serializable]
  public class ExperimentMessageType
  {
  
    public string name;
    private double help;
    public Vector3 test;
    public OperationType operation;
    public Type dataType;
    public uint id;
  }

  public static class MemoryMappedFileExperiment
  {
    private static ulong MaxWriteOffset = 0;
    private static int ConsumerHeaderSize { get; set; }

    private static int MaxWriteOffsetHeaderSize { get; set; }
    private static int HeaderSize { get; set; }
    private static ushort numberOfConsumers = 10;
    private static long cacheSpace = 268435456;
    private static long dataStorageSpace = cacheSpace - HeaderSize;

    static MemoryMappedFileExperiment()
    {
      MaxWriteOffsetHeaderSize = sizeof(ulong);
      ConsumerHeaderSize = Marshal.SizeOf(typeof(ConsumerRecord)) * numberOfConsumers;
      HeaderSize = ConsumerHeaderSize + MaxWriteOffsetHeaderSize;
    }

    public static ExperimentMessageType generateSimpleExperimentData()
    {
      var t = new ExperimentMessageType()
      {
        name = "test",
      };
      return t;
    }

    public static void DoExperiment1()
    {
      
      var file = MemoryMappedFile.CreateOrOpen("testFile-headers", HeaderSize);
      ulong offset = 34534;
      var MaxWriteOffsetStream = file.CreateViewStream(0, HeaderSize);
      

      var offsetBytes = BitConverter.GetBytes(offset);
      
      MaxWriteOffsetStream.Write(offsetBytes,0, offsetBytes.Length);

      var accessor = file.CreateViewAccessor(0, HeaderSize);
      accessor.Write(0, offset);
      var consumerRecords = new List<ConsumerRecord>()
      {
        new ConsumerRecord()
        {
          id = 12,
          lastConsumedIndex = 24
        },
        new ConsumerRecord()
        {
          id = 32532,
          lastConsumedIndex = 36
        }
      };

      accessor.WriteArray<ConsumerRecord>(MaxWriteOffsetHeaderSize, consumerRecords.ToArray(),0, consumerRecords.Count);

      ulong readOffset = 0;
      accessor.Read<ulong>(0, out readOffset);

      var x = 3;
      ConsumerRecord[] readConsumerRecords = new ConsumerRecord[x];
      accessor.ReadArray<ConsumerRecord>(MaxWriteOffsetHeaderSize, readConsumerRecords, 0, 1);

      Console.WriteLine($"returnedOffset: {readOffset}, consumer record id: {readConsumerRecords.ToString()}");
    }

    public static void DoExperiment3()
    {
      var file = MemoryMappedFile.CreateOrOpen("testFile-headers", HeaderSize);
      var accessor = file.CreateViewAccessor(0, HeaderSize);
      ulong readOffset = 0;
      accessor.Read<ulong>(0, out readOffset);

      var x = 3;
      ConsumerRecord[] readConsumerRecords = new ConsumerRecord[x];
      accessor.ReadArray<ConsumerRecord>(MaxWriteOffsetHeaderSize, readConsumerRecords, 0, 1);

      Console.WriteLine($"returnedOffset: {readOffset}, consumer record id: {readConsumerRecords.ToString()}");

    }

    public static void DoExperiment2()
    {
      var d = generateSimpleExperimentData();
      
      var formatter = new BinaryFormatter();

      

      
      var bytes = ZeroFormatterSerializer.Serialize(d);
      
      var dataSize = bytes.Length;
      var queueSize = dataSize * 50;

      using (var file = MemoryMappedFile.CreateOrOpen("testFile-headers", dataSize))
      {
        var viewStream = file.CreateViewStream(0, dataSize);

        viewStream.Write(bytes, 0, dataSize);
        viewStream.Close();

        var newViewStream = file.CreateViewStream(0, dataSize);


        var result = ZeroFormatterSerializer.Deserialize<ExperimentMessageType>(newViewStream);
        
        newViewStream.Close();
        
        var convertedResult = (ExperimentMessageType) result;
        Console.WriteLine($"a:{d.name}, b:{convertedResult.name}");
      }
    }
  }
}
