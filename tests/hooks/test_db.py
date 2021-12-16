from unittest.mock import ANY, Mock, patch

from ideafast_etl.hooks.db import DeviceType, LocalMongoHook, Record
from ideafast_etl.hooks.ucam import Device


def test_record_as_dict_equals(sample_record):
    """Test that the custom_to_dict method returns equal objects"""
    old = sample_record.as_db_dict
    device_type = DeviceType[old.pop("device_type")]
    recreation = Record(_id=None, device_type=device_type, **old)

    assert recreation == sample_record


def test_update_many_serial_not_overriding():
    """Test that the serial update does not override existing device IDs"""
    # override/disable inherited methods, including init()
    LocalMongoHook.__bases__ = (Mock,)

    db_hook = LocalMongoHook()

    result = db_hook.update_many_serial_to_deviceid("0123", "abcd", DeviceType.BTF)

    # Particularly interested in "device_id": None
    db_hook.update_many.assert_called_once_with(
        mongo_collection=ANY,
        filter_doc={"device_type": ANY, "device_serial": ANY, "device_id": None},
        update_doc=ANY,
    )


def test_update_drmuids_not_overriding():
    """Test that the drm uid update does not override existing serials"""
    # override/disable inherited methods, including init()
    LocalMongoHook.__bases__ = (Mock,)

    db_hook = LocalMongoHook()

    result = db_hook.update_drmuid_to_serial("0123", "abcd")

    # Particularly interested in "device_serial": None
    db_hook.update_many.assert_called_once_with(
        mongo_collection=ANY,
        filter_doc={"device_type": ANY, "meta.dreem_uid": ANY, "device_serial": None},
        update_doc=ANY,
    )


def test_find_unknown_deviceid_only_none(sample_db_record):
    """Test that finding unknown device IDs only returns unknown _and unique_ device ids"""
    # override/disable inherited methods, including init()
    LocalMongoHook.__bases__ = (Mock,)
    db_hook = LocalMongoHook()

    with patch.object(
        db_hook, "find", return_value=[sample_db_record() for _ in range(5)]
    ):
        result = [r for r in db_hook.find_deviceid_is_none(DeviceType.BTF)]

        assert len(result) == 1


def test_find_unknown_deviceid_only_none(sample_db_record):
    """Test that finding unknown device IDs only returns unknown _and unique_ device ids"""
    # override/disable inherited methods, including init()
    LocalMongoHook.__bases__ = (Mock,)
    db_hook = LocalMongoHook()

    with patch.object(
        db_hook, "find", return_value=[sample_db_record() for _ in range(5)]
    ):
        result = [r for r in db_hook.find_drm_deviceserial_is_none()]

        assert len(result) == 1


def test_find_notuploaded_dmps(sample_db_record):
    """Test that dmp_ids are returned even though some of the records with that ID are already uploaded"""
    # override/disable inherited methods, including init()
    LocalMongoHook.__bases__ = (Mock,)
    payload = [sample_db_record() for _ in range(5)]
    [k.update({"dmp_id": "test_id"}) for k in payload]  # set to 'ready'
    payload[2].update({"is_uploaded": True})  # fake one record to be uploaded

    db_hook = LocalMongoHook()

    with patch.object(db_hook, "find", return_value=payload):
        result = [r for r in db_hook.find_notuploaded_dmpids(DeviceType.BTF)]

        assert len(result) == 1
        assert result[0] == "test_id"


def test_hash_generation():
    """Test that hash generation is as expected with repeatable results"""

    result = Record.generate_hash("abcd", DeviceType.BTF)

    assert result == Record.generate_hash("abcd", DeviceType.BTF)
    assert result != Record.generate_hash("abce", DeviceType.BTF)
    assert result != Record.generate_hash("abcd", DeviceType.DRM)
