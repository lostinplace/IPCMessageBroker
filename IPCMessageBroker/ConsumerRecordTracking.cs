using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroFormatter;

namespace IPCMessageBroker.ConsumerRecordTracking
{
  

  [ZeroFormattable]
  public struct ConsumerRecord
  {
    public ConsumerRecord(ulong id, uint lastConsumedIndex)
    {
      this.id = id;
      this.lastConsumedIndex = lastConsumedIndex;
    }

    [Index(0)]
    public ulong id { get; set; }

    [Index(1)]
    public uint lastConsumedIndex { get; set; }
  }

  public enum RecordType
  {
    ConsumerRecordSize,
    ConsumerCount,
    LastWrittenIndex
  }


  public static class ConsumerRecordTracking
  {
    private static uint getSizeOfConsumerRecord()
    {
      var record = new ConsumerRecord();
      var result = ZeroFormatterSerializer.Serialize(record);
      return (uint)result.Length;
    }

    //uint
    public const ushort CONSUMER_RECORD_SIZE_OFFSET = 0;
    public const ushort CONSUMER_RECORD_SIZE_SIZE = sizeof(uint);

    //uint
    public const ushort CONSUMER_COUNT_OFFSET = CONSUMER_RECORD_SIZE_OFFSET + CONSUMER_RECORD_SIZE_SIZE;
    public const ushort CONSUMER_COUNT_SIZE = sizeof(uint);

    //uint
    public const ushort LAST_WRITTEN_INDEX_OFFSET = CONSUMER_COUNT_OFFSET + CONSUMER_COUNT_SIZE;
    public const ushort LAST_WRITTEN_INDEX_SIZE = sizeof(uint);

    public const ushort HEADER_SIZE = CONSUMER_RECORD_SIZE_SIZE + CONSUMER_COUNT_SIZE + LAST_WRITTEN_INDEX_SIZE;


    private static string GetConsumerRecordHeaderFileName(string ApplicationName)
    {
      return $"{ApplicationName}-Consumer-Record-Header";
    }
    
    private static string GetMutexName(string ApplicationName, RecordType recordType)
    {
      return $"{GetConsumerRecordHeaderFileName(ApplicationName)}-{recordType.ToString()}-Mutex";
    }

    private static (ushort offset, ushort size) GetOffsetAndSizeForRecordType(RecordType recordType)
    {
      switch (recordType)
      {
        case RecordType.ConsumerRecordSize:
          return (CONSUMER_RECORD_SIZE_OFFSET, CONSUMER_RECORD_SIZE_SIZE);
        case RecordType.ConsumerCount:
          return (CONSUMER_COUNT_OFFSET, CONSUMER_COUNT_SIZE);
        case RecordType.LastWrittenIndex:
          return (LAST_WRITTEN_INDEX_OFFSET, LAST_WRITTEN_INDEX_SIZE);
        default:
          return (ushort.MaxValue,ushort.MaxValue);
      }
    }

    public static void InitializeConsumerRecordHeaderCache(string ApplicationName)
    {
      var headerFileName = GetConsumerRecordHeaderFileName(ApplicationName);
      MemoryMappedFile file;
      try
      {
        file = MemoryMappedFile.CreateNew(headerFileName, HEADER_SIZE);
      }
      catch (Exception ex)
      {
        //TODO: need to figure out what happens here
        return;
      }
      var mutexName = $"{headerFileName}-Whole-File-Mutex";
      var accessor = file.CreateViewAccessor(0, HEADER_SIZE);
      var mutex = new Mutex(true, mutexName);
      mutex.WaitOne();
      accessor.Write(CONSUMER_RECORD_SIZE_OFFSET, getSizeOfConsumerRecord());
      accessor.Write(CONSUMER_COUNT_OFFSET, (ushort)0);
      accessor.Write(LAST_WRITTEN_INDEX_OFFSET, (uint)0);
      mutex.ReleaseMutex();
    }


    public static uint GetConsumerRecordSize(string ApplicationName)
    { 
      var headerFileName = GetConsumerRecordHeaderFileName(ApplicationName);
      
      using (var file = MemoryMappedFile.OpenExisting(headerFileName))
      {
        ushort tmp;
        var accessor = file.CreateViewAccessor(CONSUMER_RECORD_SIZE_OFFSET, CONSUMER_RECORD_SIZE_SIZE);
        accessor.Read<ushort>(0, out tmp);
        return tmp;
      }
    }

    public static Task<uint> GetLastWrittenIndex(string ApplicationName)
    {
      return GetMutableValueFromHeaderFile<uint>(ApplicationName, RecordType.LastWrittenIndex);
    }

    public static Task<uint> IncrementLastWrittenIndex(string ApplicationName)
    {
      return IncrementMutableValue<uint>(ApplicationName, RecordType.LastWrittenIndex);
    }

    public static Task<ushort> GetConsumerCount(string ApplicationName)
    {
      return GetMutableValueFromHeaderFile<ushort>(ApplicationName, RecordType.ConsumerCount);
    }

    private static Task<uint> IncrementMutableValue<T>(string ApplicationName, RecordType recordType) where T : struct
    {
      var source = new TaskCompletionSource<T>();
      var headerFileName = GetConsumerRecordHeaderFileName(ApplicationName);
      var mutexName = GetMutexName(ApplicationName, recordType);
      var mutex = new Mutex(false, mutexName);
      (var offset, var size) = GetOffsetAndSizeForRecordType(recordType);
      var file = MemoryMappedFile.OpenExisting(headerFileName);
      
      var task = Task.Factory.StartNew<uint>(() =>
      {
        T tmp;
        mutex.WaitOne();
        var accessor = file.CreateViewAccessor(offset, size);
        accessor.Read<T>(0, out tmp);
        var newValue = (uint)(Convert.ToInt32(tmp) + 1);
        if (newValue >= (1<<size)-1) throw new IndexOutOfRangeException("Exceeded max size of field");
        accessor.Write(0, newValue);
        mutex.ReleaseMutex();
        file.Dispose();
        return newValue;

      });
      return task;
      
    }

    private static Task<T> GetMutableValueFromHeaderFile<T>(string ApplicationName, RecordType recordType) where T : struct
    {
      var source = new TaskCompletionSource<T>();
      var headerFileName = GetConsumerRecordHeaderFileName(ApplicationName);
      var mutexName = GetMutexName(ApplicationName, recordType);
      var mutex = new Mutex(false, mutexName);
      (var offset, var size) = GetOffsetAndSizeForRecordType(recordType);
      var file = MemoryMappedFile.OpenExisting(headerFileName);
      
      var task = Task.Factory.StartNew<T>(() =>
      {
        T tmp;
        mutex.WaitOne();
        var accessor = file.CreateViewAccessor(offset, size);
        accessor.Read<T>(0, out tmp);
        mutex.ReleaseMutex();
        file.Dispose();
        return tmp;

      });
      return task;
      
    }
  }
}
