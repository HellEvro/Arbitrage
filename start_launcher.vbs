Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
launcherPy = fso.BuildPath(projectDir, "launcher\arbitrage_manager.py")
venvPython = fso.BuildPath(projectDir, ".venv\Scripts\pythonw.exe")

shell.CurrentDirectory = projectDir

If fso.FileExists(venvPython) Then
    shell.Run """" & venvPython & """ """ & launcherPy & """", 0, False
Else
    shell.Run "pythonw """ & launcherPy & """", 0, False
End If


