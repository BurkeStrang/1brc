using program;
using Xunit;

namespace tests;

public class OneBrcTests
{
  [Fact]
  public void ProcessFile_EmptyFile_ShouldReturnEmptyBraces()
  {
    var tempFile = Path.GetTempFileName();
    try
    {
      File.WriteAllText(tempFile, "");
      var output = OneBrc.ProcessFile(tempFile, 1);
      Assert.Equal("{}", output);
    }
    finally
    {
      File.Delete(tempFile);
    }
  }

  [Fact]
  public void ProcessFile_SingleLine_ShouldParseCorrectly()
  {
    var tempFile = Path.GetTempFileName();
    try
    {
      File.WriteAllText(tempFile, "Hamburg;12.3\n");
      var output = OneBrc.ProcessFile(tempFile, 1);
      Assert.Equal("{Hamburg=12.3/12.3/12.3}", output);
    }
    finally
    {
      File.Delete(tempFile);
    }
  }

  [Fact]
  public void ProcessFile_NegativeTemperature_ShouldParseCorrectly()
  {
    var tempFile = Path.GetTempFileName();
    try
    {
      File.WriteAllText(tempFile, "Berlin;-5.7\n");
      var output = OneBrc.ProcessFile(tempFile, 1);
      Assert.Equal("{Berlin=-5.7/-5.7/-5.7}", output);
    }
    finally
    {
      File.Delete(tempFile);
    }
  }

  [Fact]
  public void ProcessFile_MultipleStations_ShouldSortAlphabetically()
  {
    var tempFile = Path.GetTempFileName();
    try
    {
      File.WriteAllText(tempFile, "Zurich;10.0\nBerlin;20.0\nAmsterdam;15.0\n");
      var output = OneBrc.ProcessFile(tempFile, 1);
      Assert.StartsWith("{Amsterdam=", output);
      Assert.Contains("Berlin=", output);
      Assert.EndsWith("Zurich=10.0/10.0/10.0}", output);
    }
    finally
    {
      File.Delete(tempFile);
    }
  }

  [Fact]
  public void ProcessFile_Aggregation_ShouldCalculateMinMeanMax()
  {
    var tempFile = Path.GetTempFileName();
    try
    {
      File.WriteAllText(tempFile, "Hamburg;10.0\nHamburg;20.0\nHamburg;30.0\n");
      var output = OneBrc.ProcessFile(tempFile, 1);
      Assert.Equal("{Hamburg=10.0/20.0/30.0}", output);
    }
    finally
    {
      File.Delete(tempFile);
    }
  }

  [Fact]
  public void ProcessFile_Rounding_ShouldRoundMeanCorrectly()
  {
    var tempFile = Path.GetTempFileName();
    try
    {
      // 12.3 + 12.4 = 24.7, mean = 12.35 -> rounds to 12.4
      File.WriteAllText(tempFile, "City;12.3\nCity;12.4\n");
      var output = OneBrc.ProcessFile(tempFile, 1);
      Assert.Equal("{City=12.3/12.4/12.4}", output);
    }
    finally
    {
      File.Delete(tempFile);
    }
  }
}
