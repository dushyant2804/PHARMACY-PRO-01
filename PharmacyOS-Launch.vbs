Option Explicit

Dim shell
Set shell = CreateObject("WScript.Shell")

shell.Run """D:\pharmacy-app-v2\backend\backend-run.bat""", 0, False

WScript.Sleep 10000

shell.Run "chrome.exe --app=http://127.0.0.1:8000/", 1, False