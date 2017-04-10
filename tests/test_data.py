from dredispy.data import WrongNumberOfArgumentsError


def test_wrong_number_of_arguments_error():
    e = WrongNumberOfArgumentsError(b'dummy')
    assert e.to_resp() == b'-ERR wrong number of arguments for \'dummy\' command\r\n'
