Option Explicit

Dim shell, fso, baseDir, launcher, exitCode
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

baseDir = fso.GetParentFolderName(WScript.ScriptFullName)
launcher = fso.BuildPath(baseDir, "PharmacyOS-Desktop-Start.bat")

If Not fso.FileExists(launcher) Then
    MsgBox "PharmacyOS desktop launcher was not found:" & vbCrLf & launcher, vbCritical, "PharmacyOS"
    WScript.Quit 1
End If

exitCode = shell.Run("""" & launcher & """", 0, True)

If exitCode <> 0 Then
    MsgBox "PharmacyOS could not start cleanly." & vbCrLf & vbCrLf & _
           "Troubleshooting options:" & vbCrLf & _
           "1. Double-click PharmacyOS-Start.bat to see detailed startup output." & vbCrLf & _
           "2. Review logs in the logs folder, especially pharmacyos-local.log and pharmacyos-backend-output.log." & vbCrLf & _
           "3. Use PharmacyOS-Stop.bat before trying again.", _
           vbExclamation, "PharmacyOS"
End If

WScript.Quit exitCode
