from datetime import datetime, timezone


async def get_download_link(sharecode: str):
    # temporary for testing
    if sharecode == "CSGO-mEpFe-nWnQf-fNBas-cKFbL-LJiPK":
        return (
            3614640703184830611,
            datetime.now(timezone.utc),
            "http://replay182.valve.net/730/003614644169223438362_1052646597.dem.bz2",
        )
