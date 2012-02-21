C:\Windows\Microsoft.NET\Framework\v4.0.30319\msbuild.exe ..\Lucky.Cassandra.sln /p:Configuration=Release
copy ..\Lucky.Cassandra\bin\Release\Lucky.Cassandra.dll lib\net40 /Y
nuget pack ..\Lucky.Cassandra.nuspec
pause