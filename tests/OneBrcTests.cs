using System.Text;
using program;
using Xunit;

namespace tests;

public class OneBrcTests
{
  [Theory]
  [InlineData("1.2", 12)]
  [InlineData("-3.4", -34)]
  [InlineData("0.0", 0)]
  [InlineData("99.9", 999)]
  [InlineData("-99.9", -999)]
  [InlineData("12.3", 123)]
  [InlineData("-45.6", -456)]
  public void ParseTempBranchless_ShouldParseCorrectly(string input, int expected)
  {
    var bytes = Encoding.UTF8.GetBytes(input);
    var result = OneBrc.ParseTempBranchless(bytes);
    Assert.Equal(expected, result);
  }

  [Fact]
  public void FormatTenth_ShouldFormatCorrectly()
  {
    Assert.Equal("12.3", OneBrc.FormatTenth(123));
    Assert.Equal("-5.7", OneBrc.FormatTenth(-57));
    Assert.Equal("0.0", OneBrc.FormatTenth(0));
    Assert.Equal("99.9", OneBrc.FormatTenth(999));
    Assert.Equal("-99.9", OneBrc.FormatTenth(-999));
  }

  [Fact]
  public void StationMap_ShouldAggregateCorrectly()
  {
    var map = new StationMap();

    map.AddMeasurement("Hamburg"u8, 100);
    map.AddMeasurement("Hamburg"u8, 200);
    map.AddMeasurement("Berlin"u8, 50);
    map.AddMeasurement("Berlin"u8, 300);

    var (output, count) = map.FormatOutput();

    Assert.Equal(4, count);
    Assert.Contains("Berlin=", output);
    Assert.Contains("Hamburg=", output);
  }

  [Fact]
  public void StationMap_Merge_ShouldCombineCorrectly()
  {
    var map1 = new StationMap();
    map1.AddMeasurement("Hamburg"u8, 100);
    map1.AddMeasurement("Hamburg"u8, 200);

    var map2 = new StationMap();
    map2.AddMeasurement("Hamburg"u8, 50);
    map2.AddMeasurement("Hamburg"u8, 300);

    map1.Merge(map2);

    var (output, count) = map1.FormatOutput();

    Assert.Equal(4, count);
    // Min=50, Max=300, Avg=(100+200+50+300)/4 = 162.5 -> rounds to 163 -> 16.3
    Assert.Contains("Hamburg=5.0/16.3/30.0", output);
  }

  [Fact]
  public void StationMap_ShouldHandleManyStations()
  {
    var map = new StationMap();

    // Add many different stations to test resizing
    for (int i = 0; i < 1000; i++)
    {
      var stationBytes = Encoding.UTF8.GetBytes($"Station{i}");
      map.AddMeasurement(stationBytes, i);
    }

    var (_, count) = map.FormatOutput();
    Assert.Equal(1000, count);
  }
}
