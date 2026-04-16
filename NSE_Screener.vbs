' NSE Delivery Screener — Silent Launcher
' Double-click this to start the screener with NO console window
' Place in the same folder as screener_server.py and delivery_screener.html

Option Explicit

Dim oShell, oFSO, sDir, sPy, sHTML, sKillCmd

Set oShell = CreateObject("WScript.Shell")
Set oFSO   = CreateObject("Scripting.FileSystemObject")

' Folder where this script lives
sDir  = oFSO.GetParentFolderName(WScript.ScriptFullName)
sHTML = sDir & "\delivery_screener.html"

' Kill any existing screener server on port 5050
sKillCmd = "cmd /c for /f ""tokens=5"" %a in ('netstat -aon ^| findstr :5050 ^| findstr LISTENING') do taskkill /PID %a /F"
oShell.Run sKillCmd, 0, True

' Start Flask server silently (window hidden = 0)
sPy = "py """ & sDir & "\screener_server.py"""
oShell.Run sPy, 0, False

' Wait 3 seconds for server to come up
WScript.Sleep 3000

' Open HTML dashboard in default browser
oShell.Run "explorer """ & sHTML & """", 1, False

Set oShell = Nothing
Set oFSO   = Nothing
