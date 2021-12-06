def test_record_as_dict_equals():
    """Test that the custom_to_dict method returns equal objects"""
    pass


def test_all_db_methods_use_dict():
    """Ensure mongo inserts use the Record.as_db_dict method when storing data"""
    pass


def test_update_many_serial_not_overriding():
    """Test that the serial update does not override existing device IDs"""
    pass


def test_update_drmuids_not_overriding():
    """Test that the drm uid update does not override existing serials"""
    pass


def test_find_unknown_deviceid_only_none():
    """Test that finding unknown device IDs only returns unknown _and unique_ device ids"""
    pass


def test_find_notuploaded_dmps():
    """Test that dmp_ids are returned even though some of the records with that ID are already uploaded"""
    pass
