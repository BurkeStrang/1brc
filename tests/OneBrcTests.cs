using System.Text;
using program;
using Xunit;

namespace tests;

public class OneBrcTests
{
  [Theory]
  [InlineData("12.3", 123)]
  [InlineData("-5.7", -57)]
  [InlineData("0.0", 0)]
  [InlineData("99.9", 999)]
  [InlineData("-99.9", -999)]
  [InlineData("12.35", 124)] // Test rounding up
  [InlineData("12.34", 123)] // Test rounding down
  [InlineData("-12.35", -124)] // Test negative rounding
  [InlineData("12", 120)] // Test whole number
  [InlineData("-12", -120)] // Test negative whole number
  public void ParseTempTenths_ShouldParseCorrectly(string input, int expected)
  {
    var bytes = Encoding.UTF8.GetBytes(input);
    var result = OneBrc.ParseTempTenths(bytes);
    Assert.Equal(expected, result);
  }

  [Fact]
  public void MakeRanges_ShouldCreateCorrectRanges()
  {
    var ranges = OneBrc.MakeRanges(1000, 4);

    Assert.Equal(4, ranges.Length);
    Assert.Equal((0L, 250L), ranges[0]);
    Assert.Equal((250L, 500L), ranges[1]);
    Assert.Equal((500L, 750L), ranges[2]);
    Assert.Equal((750L, 1000L), ranges[3]);
  }

  [Fact]
  public void MakeRanges_SingleWorker_ShouldReturnSingleRange()
  {
    var ranges = OneBrc.MakeRanges(1000, 1);

    Assert.Single(ranges);
    Assert.Equal((0L, 1000L), ranges[0]);
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
  public void StationPool_ShouldInternSameStation()
  {
    var pool = OneBrc.StationPool.TlsGetOrCreate();
    var utf8 = Encoding.UTF8;

    var station1 = pool.GetStation("Hamburg"u8, utf8);
    var station2 = pool.GetStation("Hamburg"u8, utf8);

    Assert.Same(station1, station2); // Should be the same reference
    Assert.Equal("Hamburg", station1);
  }

  [Fact]
  public void StationPool_ShouldHandleDifferentStations()
  {
    var pool = OneBrc.StationPool.TlsGetOrCreate();
    var utf8 = Encoding.UTF8;

    var station1 = pool.GetStation("Hamburg"u8, utf8);
    var station2 = pool.GetStation("Berlin"u8, utf8);

    Assert.NotSame(station1, station2);
    Assert.Equal("Hamburg", station1);
    Assert.Equal("Berlin", station2);
  }

  [Fact]
  public void Stats_Merge_ShouldCombineCorrectly()
  {
    var stats1 = new OneBrc.Stats(100); // Min=100, Max=100, Sum=100, Count=1
    stats1.Add(200); // Min=100, Max=200, Sum=300, Count=2

    var stats2 = new OneBrc.Stats(50); // Min=50, Max=50, Sum=50, Count=1
    stats2.Add(300); // Min=50, Max=300, Sum=350, Count=2

    stats1.Merge(stats2);

    Assert.Equal(50, stats1.Min);
    Assert.Equal(300, stats1.Max);
    Assert.Equal(650, stats1.Sum);
    Assert.Equal(4, stats1.Count);
  }
}
