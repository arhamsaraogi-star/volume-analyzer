Set oWS = WScript.CreateObject("WScript.Shell")
sLinkFile = "C:\Users\hp\OneDrive\Desktop\Volume Analyzer.lnk"
Set oLink = oWS.CreateShortcut(sLinkFile)
oLink.TargetPath = "c:\Users\hp\OneDrive\Desktop\Volume Analyzer\update_volume_analyzer.bat"
oLink.WorkingDirectory = "c:\Users\hp\OneDrive\Desktop\Volume Analyzer"
oLink.Save
