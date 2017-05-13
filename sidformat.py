#! /usr/bin/python

"""
Library to read SID files

"""

import struct

PSIDFORMAT = ">LHHHHHHHL32s32s32sH"


class Psid:
    def __init__(self, data):
        self.data = data
        psid = struct.unpack_from(PSIDFORMAT, data)
        if psid[0] not in (0x50534944, 0x52534944):
            raise RuntimeError("wrong magic $%08X for SID" % (psid[0]))
        self.data_offset = psid[2]
        self.load_address = psid[3]
        self.init_address = psid[4]
        self.play_address = psid[5]
        self.songs = psid[6]
        self.start_song = psid[7]
        self.speed = psid[8]
        self.name = psid[9].strip('\0').decode("iso-8859-15")
        self.author = psid[10].strip('\0').decode("iso-8859-15")
        self.released = psid[11].strip('\0').decode("iso-8859-15")

    def psidHasLoadAddress(self):
        return self.load_address != 0

    def loadAddress(self):
        if not self.psidHasLoadAddress():
            return struct.unpack("<H", self.data[self.data_offset:self.data_offset +2])[0]
        return self.load_address

    def songData(self):
        if not self.psidHasLoadAddress():
            return self.data[self.data_offset + 2:]
        else:
            return self.data[self.data_offset:]

    def songDataLength(self):
        return len(self.songData())
        
if __name__ == "__main__":
    pass
