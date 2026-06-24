Option Explicit

Dim shell, fso, baseDir, backendBat
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

baseDir = fso.GetParentFolderName(WScript.ScriptFullName)
backendBat = fso.BuildPath(baseDir, "PharmacyOS-Backend-Hidden.bat")

If Not fso.FileExists(backendBat) Then
    WScript.Quit 1
End If

shell.Run """" & backendBat & """", 0, False
WScript.Quit 0
