using program;
using Xunit;

namespace tests;

public class IntegrationTests
{
  [Fact]
  public void ProcessFile_SampleData_ShouldProduceCorrectOutput()
  {
    // Get the test data file path
    var testDataPath = Path.Combine(GetTestDirectory(), "sample-data.txt");

    // Process the file
    var output = OneBrc.ProcessFile(testDataPath, 1);

    // Verify the output format is correct
    Assert.StartsWith("{", output);
    Assert.EndsWith("}", output);

    // Check key stations with expected values based on sample-data.txt
    // Hamburg: 12.0, -2.3, -7.3, 23.1 -> min=-7.3, avg=6.4, max=23.1
    // Istanbul: 6.2, 23.0 -> min=6.2, avg=14.6, max=23.0
    // Palembang: 38.8, 35.6 -> min=35.6, avg=37.2, max=38.8
    // Roseau: 34.4, 31.8 -> min=31.8, avg=33.1, max=34.4
    AssertCity(output, "Bridgetown", "26.9/26.9/26.9");
    AssertCity(output, "St. John's", "15.2/15.2/15.2");
    AssertCity(output, "Hamburg", "-7.3/6.4/23.1");
    AssertCity(output, "Istanbul", "6.2/14.6/23.0");
    AssertCity(output, "Palembang", "35.6/37.2/38.8");
    AssertCity(output, "Roseau", "31.8/33.1/34.4");

    // Verify the format uses the correct separators
    Assert.Contains("=", output);
    Assert.Contains("/", output);
    Assert.Contains(", ", output);
  }

  private static void AssertCity(string output, string city, string expectedStats)
  {
    var expected = $"{city}={expectedStats}";

    // Remove curly braces and split by comma regardless of spaces
    var entries = output.Trim('{', '}')
                        .Split(',', StringSplitOptions.RemoveEmptyEntries)
                        .Select(x => x.Trim());

    var actual = entries.FirstOrDefault(x => x.StartsWith(city + "="));

    Assert.Equal(expected, actual ?? "<not found>");
  }

  [Fact]
  public void ProcessFile_SampleData_MultipleWorkers_ShouldProduceValidOutput()
  {
    var testDataPath = Path.Combine(GetTestDirectory(), "sample-data.txt");

    var output1 = OneBrc.ProcessFile(testDataPath, 1);
    var output2 = OneBrc.ProcessFile(testDataPath, 2);

    // Both outputs should still be well-formed
    Assert.StartsWith("{", output1);
    Assert.EndsWith("}", output1);
    Assert.StartsWith("{", output2);
    Assert.EndsWith("}", output2);

    // Both should contain valid data
    Assert.Contains("=", output1);
    Assert.Contains("/", output1);
    Assert.Contains("=", output2);
    Assert.Contains("/", output2);
  }

  [Fact]
  public void ProcessFile_EdgeCases_ShouldHandleCorrectly()
  {
    // Create a temporary file with edge cases
    // Note: 1BRC spec uses exactly one decimal place
    var tempFile = Path.GetTempFileName();
    try
    {
      File.WriteAllText(tempFile,
          "A;0.0\n" +
          "B;-0.1\n" +
          "C;99.9\n" +
          "D;-99.9\n" +
          "E;12.3\n" +
          "E;12.4\n");

      var output = OneBrc.ProcessFile(tempFile, 1);

      // E: min=12.3, max=12.4, avg=(123+124)/2=123.5 rounds to 124 -> 12.4
      Assert.Contains("A=0.0/0.0/0.0", output);
      Assert.Contains("B=-0.1/-0.1/-0.1", output);
      Assert.Contains("C=99.9/99.9/99.9", output);
      Assert.Contains("D=-99.9/-99.9/-99.9", output);
      Assert.Contains("E=12.3/12.4/12.4", output);
    }
    finally
    {
      File.Delete(tempFile);
    }
  }

  [Fact]
  public void ProcessFile_RealDataSubset_ShouldProcessWithoutErrors()
  {
    // Test with actual measurement data if available
    var realDataPath = Path.Combine(GetProjectRoot(), "measurements-10000000.txt");

    if (File.Exists(realDataPath))
    {
      // Process just the first 1000 lines to verify it works
      var tempFile = Path.GetTempFileName();
      try
      {
        var lines = File.ReadLines(realDataPath).Take(1000);
        File.WriteAllLines(tempFile, lines);

        var output = OneBrc.ProcessFile(tempFile, 2);

        Assert.StartsWith("{", output);
        Assert.EndsWith("}", output);
        Assert.Contains("=", output);
        Assert.Contains("/", output);
      }
      finally
      {
        File.Delete(tempFile);
      }
    }
    else
    {
      // Skip test if real data is not available
      Assert.True(true, "Real measurement data not available, skipping test");
    }
  }

  [Fact]
  public void ProcessFile_10MillionRows_ShouldProduceCorrectOutput()
  {
    // Test with full 10 million row dataset if available
    var dataPath = Path.Combine(GetProjectRoot(), "measurements-10000000.txt");

    if (!File.Exists(dataPath))
    {
      // Skip test if data is not available
      Assert.True(true, "10 million row dataset not available, skipping test");
      return;
    }

    var output = OneBrc.ProcessFile(dataPath, 6);

    // Verify output format
    Assert.StartsWith("{", output);
    Assert.EndsWith("}", output);

    // Verify specific city statistics from known expected output
    // These values are from the verified AOT run
    AssertCity(output, "Abha", "-21.7/18.0/56.8");
    AssertCity(output, "Abidjan", "-12.4/25.9/64.2");
    AssertCity(output, "Amsterdam", "-30.1/10.1/48.9");
    AssertCity(output, "Baghdad", "-22.0/22.7/63.6");
    AssertCity(output, "Bangkok", "-8.5/28.6/75.7");
    AssertCity(output, "Berlin", "-35.1/10.4/50.8");
    AssertCity(output, "Cairo", "-24.9/21.5/60.6");
    AssertCity(output, "Chicago", "-35.9/9.8/52.7");
    AssertCity(output, "Dubai", "-14.8/27.0/64.8");
    AssertCity(output, "Hamburg", "-34.9/9.7/51.5");
    AssertCity(output, "Hong Kong", "-16.8/23.4/65.7");
    AssertCity(output, "London", "-31.0/11.4/50.8");
    AssertCity(output, "Los Angeles", "-21.9/18.7/65.2");
    AssertCity(output, "Mumbai", "-12.2/27.1/62.1");
    AssertCity(output, "New York City", "-27.1/12.9/54.8");
    AssertCity(output, "Paris", "-31.2/12.3/58.0");
    AssertCity(output, "Singapore", "-14.5/27.1/68.4");
    AssertCity(output, "Sydney", "-24.2/17.7/58.7");
    AssertCity(output, "Tokyo", "-24.1/15.4/54.7");
    AssertCity(output, "Zürich", "-31.6/9.3/50.0");
  }

  [Fact]
  public void ProcessFile_10MillionRows_DifferentWorkerCounts_ShouldProduceSameOutput()
  {
    // Test that different worker counts produce identical results
    var dataPath = Path.Combine(GetProjectRoot(), "measurements-10000000.txt");

    if (!File.Exists(dataPath))
    {
      Assert.True(true, "10 million row dataset not available, skipping test");
      return;
    }

    var output1 = OneBrc.ProcessFile(dataPath, 1);
    var output3 = OneBrc.ProcessFile(dataPath, 3);
    var output6 = OneBrc.ProcessFile(dataPath, 6);

    // All worker counts should produce identical output
    Assert.Equal(output1, output3);
    Assert.Equal(output1, output6);

  }

  private static string GetTestDirectory()
  {
    return Path.GetDirectoryName(typeof(IntegrationTests).Assembly.Location)
           ?? throw new InvalidOperationException("Could not determine test directory");
  }

  private static string GetProjectRoot()
  {
    var dir = new DirectoryInfo(GetTestDirectory());
    while (dir != null && !File.Exists(Path.Combine(dir.FullName, "1brc.slnx")))
    {
      dir = dir.Parent;
    }
    return dir?.FullName ?? throw new DirectoryNotFoundException("Could not find project root");
  }
}