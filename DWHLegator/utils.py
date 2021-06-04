
def IfNoneThenNull(inVal,inFormat='NULL'):
    if inVal is None:
        return inFormat
    else:
        return inVal
