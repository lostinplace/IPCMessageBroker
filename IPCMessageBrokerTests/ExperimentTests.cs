using IPCMessageBroker;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IPCMessageBrokerTests
{
  [TestClass]
  public class ExperimentTests
  {
    [TestMethod]
    public void TestExperiment1()
    {
      MemoryMappedFileExperiment.DoExperiment1();
      Assert.IsTrue(true);
    }

    [TestMethod]
    public void TestExperiment2()
    {
      MemoryMappedFileExperiment.DoExperiment2();
    }

    [TestMethod]
    public void TestExperiment3()
    {
      MemoryMappedFileExperiment.DoExperiment3();
    }
  }
}
