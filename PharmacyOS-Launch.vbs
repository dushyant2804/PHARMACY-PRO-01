Option Explicit

Dim shell, exitCode
Set shell = CreateObject("WScript.Shell")

exitCode = shell.Run("""D:\pharmacy-app-v2\backend\backend-run.bat""", 0, False)

If exitCode <> 0 Then
    MsgBox "PharmacyOS backend could not be started.", vbCritical, "PharmacyOS"
End If

WScript.Quit exitCode
