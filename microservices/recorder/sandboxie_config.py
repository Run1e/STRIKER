colors = [
    "#0000FF",
    "#00FF00",
    "#FF0000",
    "#00FFFF",
]


def make_config(data_dir, temp_dir, boxes):
    sections = [
        """
[UserSettings_0D280224]
SbieCtrl_AutoRunSoftCompat=n
SbieCtrl_HideWindowNotify=n
SbieCtrl_ReloadConfNotify=n
SbieCtrl_TerminateWarn=n

[DefaultBox]
Enabled=y
"""
    ]

    for box, color in zip(boxes, colors):
        sections.append(make_box(box=box, data_dir=data_dir, temp_dir=temp_dir, color=color))

    return "\n".join(sections)


def make_box(**kwargs):
    return """[{box}]
Enabled=y
ConfigLevel=9
AutoRecover=y
BlockNetworkFiles=y
Template=OpenSmartCard
Template=OpenBluetooth
Template=SkipHook
Template=FileCopy
Template=qWave
Template=BlockPorts
Template=LingerPrograms
Template=AutoRecoverIgnore
BorderColor={color},on,6
BoxNameTitle=y
AutoDelete=y
NeverDelete=n
CopyLimitKb=-1
CopyLimitSilent=y
OpenFilePath={data_dir}
OpenFilePath={temp_dir}""".format(
        **kwargs
    )
