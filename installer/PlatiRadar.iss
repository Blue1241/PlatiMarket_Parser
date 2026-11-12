#define AppName "Plati Radar"
#define AppVersion "0.2.0"
#define AppPublisher "Plati Radar"
#define AppExeName "plati_rust_parser.exe"

[Setup]
AppId={{D52155A8-8616-4E7E-8F68-B458D227AD01}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={localappdata}\Programs\Plati Radar
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
OutputDir=..\dist
OutputBaseFilename=PlatiRadar-Setup-{#AppVersion}
SetupIconFile=..\assets\plati-radar.ico
UninstallDisplayIcon={app}\{#AppExeName}
PrivilegesRequired=lowest
ArchitecturesInstallIn64BitMode=x64
WizardStyle=modern
Compression=lzma2
SolidCompression=yes

[Languages]
Name: "russian"; MessagesFile: "compiler:Languages\Russian.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Создать значок на рабочем столе"; GroupDescription: "Дополнительные значки:"; Flags: unchecked

[Files]
Source: "..\target\release\plati_rust_parser.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\LICENSE"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\assets\plati-radar.ico"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Plati Radar"; Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\plati-radar.ico"
Name: "{autodesktop}\Plati Radar"; Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\plati-radar.ico"; Tasks: desktopicon

[Run]
Filename: "{app}\{#AppExeName}"; Description: "Запустить Plati Radar"; Flags: postinstall nowait skipifsilent
