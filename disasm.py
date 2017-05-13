#! /usr/bin/python

import os
import sys
import argparse
import math
import json
import random
import sidformat

MODE_IMMEDIATE        =  0
MODE_ABSOLUTE         =  1
MODE_ZERO_PAGE        =  2
MODE_ACCUMULATOR      =  3
MODE_IMPLIED          =  4
MODE_INDEXED_INDIRECT =  5
MODE_INDIRECT_INDEXED =  6
MODE_ZERO_PAGE_X      =  7
MODE_ZERO_PAGE_Y      =  8
MODE_ABSOLUTE_X       =  9
MODE_ABSOLUTE_Y       = 10
MODE_RELATIVE         = 11
MODE_INDIRECT         = 12


OPCODES = [
    [ "BRK", MODE_IMPLIED          ],       # 00
    [ "ORA", MODE_INDEXED_INDIRECT ],       # 01
    [ "kil", MODE_IMPLIED          ],       # 02
    [ "slo", MODE_INDIRECT_INDEXED ],       # 03
    [ "dop", MODE_ZERO_PAGE        ],       # 04
    [ "ORA", MODE_ZERO_PAGE        ],       # 05
    [ "ASL", MODE_ZERO_PAGE        ],       # 06
    [ "slo", MODE_ZERO_PAGE        ],       # 07
    [ "PHP", MODE_IMPLIED          ],       # 08
    [ "ORA", MODE_IMMEDIATE        ],       # 09
    [ "ASL", MODE_ACCUMULATOR      ],       # 0A
    [ "aac", MODE_IMMEDIATE        ],       # 0B
    [ "top", MODE_ABSOLUTE         ],       # 0C
    [ "ORA", MODE_ABSOLUTE         ],       # 0D
    [ "ASL", MODE_ABSOLUTE         ],       # 0E
    [ "slo", MODE_ABSOLUTE         ],       # 0F
    [ "BPL", MODE_RELATIVE         ],       # 10
    [ "ORA", MODE_INDIRECT_INDEXED ],       # 11
    [ "kil", MODE_IMPLIED          ],       # 12
    [ "slo", MODE_INDEXED_INDIRECT ],       # 13
    [ "dop", MODE_ZERO_PAGE_X      ],       # 14
    [ "ORA", MODE_ZERO_PAGE_X      ],       # 15
    [ "ASL", MODE_ZERO_PAGE_X      ],       # 16
    [ "slo", MODE_ZERO_PAGE_X      ],       # 17
    [ "CLC", MODE_IMPLIED          ],       # 18
    [ "ORA", MODE_ABSOLUTE_Y       ],       # 19
    [ "nop", MODE_IMPLIED          ],       # 1A
    [ "slo", MODE_ABSOLUTE_Y       ],       # 1B
    [ "top", MODE_ABSOLUTE_X       ],       # 1C
    [ "ORA", MODE_ABSOLUTE_X       ],       # 1D
    [ "ASL", MODE_ABSOLUTE_X       ],       # 1E
    [ "slo", MODE_ABSOLUTE_X       ],       # 1F
    [ "JSR", MODE_ABSOLUTE         ],       # 20
    [ "AND", MODE_INDEXED_INDIRECT ],       # 21
    [ "kil", MODE_IMPLIED          ],       # 22
    [ "rla", MODE_INDEXED_INDIRECT ],       # 23
    [ "BIT", MODE_ZERO_PAGE        ],       # 24
    [ "AND", MODE_ZERO_PAGE        ],       # 25
    [ "ROL", MODE_ZERO_PAGE        ],       # 26
    [ "rla", MODE_ZERO_PAGE        ],       # 27
    [ "PLP", MODE_IMPLIED          ],       # 28
    [ "AND", MODE_IMMEDIATE        ],       # 29
    [ "ROL", MODE_ACCUMULATOR      ],       # 2A
    [ "aac", MODE_IMMEDIATE        ],       # 2B
    [ "BIT", MODE_ABSOLUTE         ],       # 2C
    [ "AND", MODE_ABSOLUTE         ],       # 2D
    [ "ROL", MODE_ABSOLUTE         ],       # 2E
    [ "rla", MODE_ABSOLUTE         ],       # 2F
    [ "BMI", MODE_RELATIVE         ],       # 30
    [ "AND", MODE_INDIRECT_INDEXED ],       # 31
    [ "kil", MODE_IMPLIED          ],       # 32
    [ "rla", MODE_INDIRECT_INDEXED ],       # 33
    [ "dop", MODE_ZERO_PAGE_X      ],       # 34
    [ "AND", MODE_ZERO_PAGE_X      ],       # 35
    [ "ROL", MODE_ZERO_PAGE_X      ],       # 36
    [ "rla", MODE_ZERO_PAGE_X      ],       # 37
    [ "SEC", MODE_IMPLIED          ],       # 38
    [ "AND", MODE_ABSOLUTE_Y       ],       # 39
    [ "nop", MODE_IMPLIED          ],       # 3A
    [ "rla", MODE_ABSOLUTE_Y       ],       # 3B
    [ "top", MODE_ABSOLUTE_X       ],       # 3C
    [ "AND", MODE_ABSOLUTE_X       ],       # 3D
    [ "ROL", MODE_ABSOLUTE_X       ],       # 3E
    [ "rla", MODE_ABSOLUTE_X       ],       # 3F
    [ "RTI", MODE_IMPLIED          ],       # 40
    [ "EOR", MODE_INDEXED_INDIRECT ],       # 41
    [ "kil", MODE_IMPLIED          ],       # 42
    [ "sre", MODE_INDEXED_INDIRECT ],       # 43
    [ "dop", MODE_ZERO_PAGE        ],       # 44
    [ "EOR", MODE_ZERO_PAGE        ],       # 45
    [ "LSR", MODE_ZERO_PAGE        ],       # 46
    [ "sre", MODE_ZERO_PAGE        ],       # 47
    [ "PHA", MODE_IMPLIED          ],       # 48
    [ "EOR", MODE_IMMEDIATE        ],       # 49
    [ "LSR", MODE_ACCUMULATOR      ],       # 4A
    [ "asr", MODE_IMMEDIATE        ],       # 4B
    [ "JMP", MODE_ABSOLUTE         ],       # 4C
    [ "EOR", MODE_ABSOLUTE         ],       # 4D
    [ "LSR", MODE_ABSOLUTE         ],       # 4E
    [ "sre", MODE_ABSOLUTE         ],       # 4F
    [ "BVC", MODE_RELATIVE         ],       # 50
    [ "EOR", MODE_INDIRECT_INDEXED ],       # 51
    [ "kil", MODE_IMPLIED          ],       # 52
    [ "sre", MODE_INDIRECT_INDEXED ],       # 53
    [ "dop", MODE_ZERO_PAGE_X      ],       # 54
    [ "EOR", MODE_ZERO_PAGE_X      ],       # 55
    [ "LSR", MODE_ZERO_PAGE_X      ],       # 56
    [ "sre", MODE_ZERO_PAGE_X      ],       # 57
    [ "CLI", MODE_IMPLIED          ],       # 58
    [ "EOR", MODE_ABSOLUTE_Y       ],       # 59
    [ "nop", MODE_IMPLIED          ],       # 5A
    [ "sre", MODE_ABSOLUTE_Y       ],       # 5B
    [ "top", MODE_ABSOLUTE_X       ],       # 5C
    [ "EOR", MODE_ABSOLUTE_X       ],       # 5D
    [ "LSR", MODE_ABSOLUTE_X       ],       # 5E
    [ "sre", MODE_ABSOLUTE_X       ],       # 5F
    [ "RTS", MODE_IMPLIED          ],       # 60
    [ "ADC", MODE_INDEXED_INDIRECT ],       # 61
    [ "kil", MODE_IMPLIED          ],       # 62
    [ "rra", MODE_INDEXED_INDIRECT ],       # 63
    [ "dop", MODE_ZERO_PAGE        ],       # 64
    [ "ADC", MODE_ZERO_PAGE        ],       # 65
    [ "ROR", MODE_ZERO_PAGE        ],       # 66
    [ "rra", MODE_ZERO_PAGE        ],       # 67
    [ "PLA", MODE_IMPLIED          ],       # 68
    [ "ADC", MODE_IMMEDIATE        ],       # 69
    [ "ROR", MODE_ACCUMULATOR      ],       # 6A
    [ "arr", MODE_IMMEDIATE        ],       # 6B
    [ "JMP", MODE_INDIRECT         ],       # 6C
    [ "ADC", MODE_ABSOLUTE         ],       # 6D
    [ "ROR", MODE_ABSOLUTE         ],       # 6E
    [ "rra", MODE_ABSOLUTE         ],       # 6F
    [ "BVS", MODE_RELATIVE         ],       # 70
    [ "ADC", MODE_INDIRECT_INDEXED ],       # 71
    [ "kil", MODE_IMPLIED          ],       # 72
    [ "rra", MODE_INDIRECT_INDEXED ],       # 73
    [ "dop", MODE_ZERO_PAGE_X      ],       # 74
    [ "ADC", MODE_ZERO_PAGE_X      ],       # 75
    [ "ROR", MODE_ZERO_PAGE_X      ],       # 76
    [ "rra", MODE_ZERO_PAGE_X      ],       # 77
    [ "SEI", MODE_IMPLIED          ],       # 78
    [ "ADC", MODE_ABSOLUTE_Y       ],       # 79
    [ "nop", MODE_IMPLIED          ],       # 7A
    [ "rra", MODE_ABSOLUTE_Y       ],       # 7B
    [ "top", MODE_ABSOLUTE_X       ],       # 7C
    [ "ADC", MODE_ABSOLUTE_X       ],       # 7D
    [ "ROR", MODE_ABSOLUTE_X       ],       # 7E
    [ "rra", MODE_ABSOLUTE_X       ],       # 7F
    [ "dop", MODE_IMMEDIATE        ],       # 80
    [ "STA", MODE_INDEXED_INDIRECT ],       # 81
    [ "dop", MODE_IMMEDIATE        ],       # 82
    [ "aax", MODE_INDEXED_INDIRECT ],       # 83
    [ "STY", MODE_ZERO_PAGE        ],       # 84
    [ "STA", MODE_ZERO_PAGE        ],       # 85
    [ "STX", MODE_ZERO_PAGE        ],       # 86
    [ "aax", MODE_ZERO_PAGE        ],       # 87
    [ "DEY", MODE_IMPLIED          ],       # 88
    [ "dop", MODE_IMMEDIATE        ],       # 89
    [ "TXA", MODE_IMPLIED          ],       # 8A
    [ "xaa", MODE_IMMEDIATE        ],       # 8B
    [ "STY", MODE_ABSOLUTE         ],       # 8C
    [ "STA", MODE_ABSOLUTE         ],       # 8D
    [ "STX", MODE_ABSOLUTE         ],       # 8E
    [ "aax", MODE_ABSOLUTE         ],       # 8F
    [ "BCC", MODE_RELATIVE         ],       # 90
    [ "STA", MODE_INDIRECT_INDEXED ],       # 91
    [ "kil", MODE_IMPLIED          ],       # 92
    [ "axa", MODE_INDIRECT_INDEXED ],       # 93
    [ "STY", MODE_ZERO_PAGE_X      ],       # 94
    [ "STA", MODE_ZERO_PAGE_X      ],       # 95
    [ "STX", MODE_ZERO_PAGE_Y      ],       # 96
    [ "aax", MODE_ZERO_PAGE_Y      ],       # 97
    [ "TYA", MODE_IMPLIED          ],       # 98
    [ "STA", MODE_ABSOLUTE_Y       ],       # 99
    [ "TXS", MODE_IMPLIED          ],       # 9A
    [ "xas", MODE_ABSOLUTE_Y       ],       # 9B
    [ "sya", MODE_ABSOLUTE_X       ],       # 9C
    [ "STA", MODE_ABSOLUTE_X       ],       # 9D
    [ "sxa", MODE_ABSOLUTE_Y       ],       # 9E
    [ "axa", MODE_ABSOLUTE_Y       ],       # 9F
    [ "LDY", MODE_IMMEDIATE        ],       # A0
    [ "LDA", MODE_INDEXED_INDIRECT ],       # A1
    [ "LDX", MODE_IMMEDIATE        ],       # A2
    [ "lax", MODE_INDEXED_INDIRECT ],       # A3
    [ "LDY", MODE_ZERO_PAGE        ],       # A4
    [ "LDA", MODE_ZERO_PAGE        ],       # A5
    [ "LDX", MODE_ZERO_PAGE        ],       # A6
    [ "lax", MODE_ZERO_PAGE        ],       # A7
    [ "TAY", MODE_IMPLIED          ],       # A8
    [ "LDA", MODE_IMMEDIATE        ],       # A9
    [ "TAX", MODE_IMPLIED          ],       # AA
    [ "atx", MODE_IMPLIED          ],       # AB
    [ "LDY", MODE_ABSOLUTE         ],       # AC
    [ "LDA", MODE_ABSOLUTE         ],       # AD
    [ "LDX", MODE_ABSOLUTE         ],       # AE
    [ "lax", MODE_ABSOLUTE         ],       # AF
    [ "BCS", MODE_RELATIVE         ],       # B0
    [ "LDA", MODE_INDIRECT_INDEXED ],       # B1
    [ "kil", MODE_IMPLIED          ],       # B2
    [ "lax", MODE_INDIRECT_INDEXED ],       # B3
    [ "LDY", MODE_ZERO_PAGE_X      ],       # B4
    [ "LDA", MODE_ZERO_PAGE_X      ],       # B5
    [ "LDX", MODE_ZERO_PAGE_Y      ],       # B6
    [ "lax", MODE_ZERO_PAGE_Y      ],       # B7
    [ "CLV", MODE_IMPLIED          ],       # B8
    [ "LDA", MODE_ABSOLUTE_Y       ],       # B9
    [ "TSX", MODE_IMPLIED          ],       # BA
    [ "lar", MODE_ABSOLUTE_Y       ],       # BB
    [ "LDY", MODE_ABSOLUTE_X       ],       # BC
    [ "LDA", MODE_ABSOLUTE_X       ],       # BD
    [ "LDX", MODE_ABSOLUTE_Y       ],       # BE
    [ "lax", MODE_ABSOLUTE_Y       ],       # BF
    [ "CPY", MODE_IMMEDIATE        ],       # C0
    [ "CMP", MODE_INDEXED_INDIRECT ],       # C1
    [ "dop", MODE_IMMEDIATE        ],       # C2
    [ "dcp", MODE_INDEXED_INDIRECT ],       # C3
    [ "CPY", MODE_ZERO_PAGE        ],       # C4
    [ "CMP", MODE_ZERO_PAGE        ],       # C5
    [ "DEC", MODE_ZERO_PAGE        ],       # C6
    [ "dcp", MODE_ZERO_PAGE        ],       # C7
    [ "INY", MODE_IMPLIED          ],       # C8
    [ "CMP", MODE_IMMEDIATE        ],       # C9
    [ "DEX", MODE_IMPLIED          ],       # CA
    [ "axs", MODE_IMMEDIATE        ],       # CB
    [ "CPY", MODE_ABSOLUTE         ],       # CC
    [ "CMP", MODE_ABSOLUTE         ],       # CD
    [ "DEC", MODE_ABSOLUTE         ],       # CE
    [ "dcp", MODE_ABSOLUTE         ],       # CF
    [ "BNE", MODE_RELATIVE         ],       # D0
    [ "CMP", MODE_INDIRECT_INDEXED ],       # D1
    [ "kil", MODE_IMPLIED          ],       # D2
    [ "dcp", MODE_INDIRECT_INDEXED ],       # D3
    [ "dop", MODE_ZERO_PAGE_X      ],       # D4
    [ "CMP", MODE_ZERO_PAGE_X      ],       # D5
    [ "DEC", MODE_ZERO_PAGE_X      ],       # D6
    [ "dcp", MODE_ZERO_PAGE_X      ],       # D7
    [ "CLD", MODE_IMPLIED          ],       # D8
    [ "CMP", MODE_ABSOLUTE_Y       ],       # D9
    [ "nop", MODE_IMPLIED          ],       # DA
    [ "dcp", MODE_ABSOLUTE_Y       ],       # DB
    [ "top", MODE_ABSOLUTE_X       ],       # DC
    [ "CMP", MODE_ABSOLUTE_X       ],       # DD
    [ "DEC", MODE_ABSOLUTE_X       ],       # DE
    [ "dcp", MODE_ABSOLUTE_X       ],       # DF
    [ "CPX", MODE_IMMEDIATE        ],       # E0
    [ "SBC", MODE_INDEXED_INDIRECT ],       # E1
    [ "dop", MODE_IMMEDIATE        ],       # E2
    [ "isc", MODE_INDEXED_INDIRECT ],       # E3
    [ "CPX", MODE_ZERO_PAGE        ],       # E4
    [ "SBC", MODE_ZERO_PAGE        ],       # E5
    [ "INC", MODE_ZERO_PAGE        ],       # E6
    [ "isc", MODE_ZERO_PAGE        ],       # E7
    [ "INX", MODE_IMPLIED          ],       # E8
    [ "SBC", MODE_IMMEDIATE        ],       # E9
    [ "NOP", MODE_IMPLIED          ],       # EA
    [ "sbc", MODE_IMMEDIATE        ],       # EB
    [ "CPX", MODE_ABSOLUTE         ],       # EC
    [ "SBC", MODE_ABSOLUTE         ],       # ED
    [ "INC", MODE_ABSOLUTE         ],       # EE
    [ "isc", MODE_ABSOLUTE         ],       # EF
    [ "BEQ", MODE_RELATIVE         ],       # F0
    [ "SBC", MODE_INDIRECT_INDEXED ],       # F1
    [ "kil", MODE_IMPLIED          ],       # F2
    [ "isc", MODE_INDIRECT_INDEXED ],       # F3
    [ "dop", MODE_ZERO_PAGE_X      ],       # F4
    [ "SBC", MODE_ZERO_PAGE_X      ],       # F5
    [ "INC", MODE_ZERO_PAGE_X      ],       # F6
    [ "isc", MODE_ZERO_PAGE_X      ],       # F7
    [ "SED", MODE_IMPLIED          ],       # F8
    [ "SBC", MODE_ABSOLUTE_Y       ],       # F9
    [ "nop", MODE_IMPLIED          ],       # FA
    [ "isc", MODE_ABSOLUTE_Y       ],       # FB
    [ "top", MODE_ABSOLUTE_X       ],       # FC
    [ "SBC", MODE_ABSOLUTE_X       ],       # FD
    [ "INC", MODE_ABSOLUTE_X       ],       # FE
    [ "isc", MODE_ABSOLUTE_X       ],       # FF
    []
    ]


def disassemble(data, org, pos, length):
    """
    Disassemble data.

    Each entry in the disassembly is a tuple of::
    
    (address, hexdump, disassembly, opcode)

    @param data: array of data bytes
    @param org: origin of data array
    @param pos: position in array to start disassembly
    @param length: number of bytes to disassemble
    @return: array of disassembled codes
    """
    lines = []
    posend = pos + length
    while pos < posend - 2:
        oldpos = pos
        opcode = data[pos]
        addressing = OPCODES[opcode][1]
        line = "%s " % (OPCODES[opcode][0])
        if addressing == MODE_IMMEDIATE:
            pos += 1
            line += "#$%02X" % data[pos]
        elif addressing == MODE_ABSOLUTE        :
            line += "$%04X" % (data[pos + 1] | data[pos + 2] << 8)
            pos += 2
        elif addressing == MODE_ZERO_PAGE       :
            pos += 1
            line += "$%02X" % data[pos]
        elif addressing == MODE_ACCUMULATOR     :
            pass
        elif addressing == MODE_IMPLIED         :
            pass
        elif addressing == MODE_INDEXED_INDIRECT:
            pos += 1
            line += "($%02X,X)" % data[pos]
        elif addressing == MODE_INDIRECT_INDEXED:
            pos += 1
            line += "($%02X),Y" % data[pos]
        elif addressing == MODE_ZERO_PAGE_X     :
            pos += 1
            line += "$%02X,X" % data[pos]
        elif addressing == MODE_ZERO_PAGE_Y     :
            pos += 1
            line += "$%02X,Y" % data[pos]
        elif addressing == MODE_ABSOLUTE_X      :
            line += "$%04X,X" % (data[pos + 1] | data[pos + 2] << 8)
            pos += 2
        elif addressing == MODE_ABSOLUTE_Y      :
            line += "$%04X,Y" % (data[pos + 1] | data[pos + 2] << 8)
            pos += 2
        elif addressing == MODE_RELATIVE        :
            pos += 1
            tmp = data[pos]
            tmp = (tmp & 0x7F) - (tmp & ~0x7F)
            line += "$%04X (%+d)" % (pos + tmp + 1 + org, tmp)
        elif addressing == MODE_INDIRECT        :
            line += "($%04X)" % (data[pos + 1] | data[pos + 2] << 8)
            pos += 2
        else:
            raise RuntimeError("malfunction")
        pos += 1
        _ = (oldpos + org,
             " ".join("%02X" % data[i] for i in range(oldpos, pos)),
             line,
             opcode,
             line[0].islower()
            )
        lines.append(_)
    return lines


def dis_file(fname, modelname):
    model = json.load(file(modelname))
    all_classifications = []
    with file(fname) as inp:
        #data = sidformat.Psid([ord(_) for _ in inp.read() + "\0\0"])
        sid = sidformat.Psid(inp.read())
        data = [ord(_) for _ in sid.songData() + "\0\0"]
        disassembly = disassemble(data, sid.loadAddress(), 0, len(data) - 2)
        frmt = "\t$%04X \t %-10s \t %s"
        frmt = "\t$%04X \t \33[34m%-10s\33[0m \t %s"
        frmt_ill = "\t$%04X \t \33[34m%-10s\33[0m \t \33[31m%s\33[0m"
        for opstep in range(0, len(disassembly), 8):
            opslice = disassembly[opstep:opstep + 8]
            classifi, _, opcodes, features = analyse_data(model, opslice, 0)
            out = [(frmt if not line[4] else frmt_ill) % (line[0], line[1], line[2]) for line in opslice]
            print("\n".join([classifi + i for i in out]))
            all_classifications.append(classifi)
    return all_classifications


def analyse_data(model, disassembly, pos):
    opcodes = [_[3] for _ in disassembly]
    features = [0] * 256
    for opcode in opcodes:
        features[opcode] = 1
    scores = {i : 0 for i in model.keys()}
    for idx, feature in enumerate(features):
        for key in scores.keys():
            scores[key] += model[key][idx] * feature
    maxitem = 0
    items = scores.items()
    for pos, item in enumerate(items):
        if item[1] > items[maxitem][1]:
            maxitem = pos
    return (items[maxitem][0], scores, opcodes, features)


def decision(model, fname, retries):
    """
    Update decision model with file fname.
    
    @param model: model dictionary
    @param fname: filename to read PSID
    @param retries: number of retries per file
    @return: updated model dictionary
    """
    with file(fname) as inp:
        sid = sidformat.Psid(inp.read())
        data = [ord(_) for _ in sid.songData() + "\0\0"]
    frmt = "\t$%04X  %-10s %s"
    pos = -1
    for count in range(retries + 1):
        if pos < 0:
            pos = random.randint(0, len(data) - 16)
        disassembly = disassemble(data, sid.loadAddress(), pos, 16 + 2)
        classifi, _, opcodes, features = analyse_data(model, disassembly, pos)
        print("\n".join([frmt % (line[0], line[1], line[2]) for line in disassembly]))
        print count, classifi
        print "(Yes/No/Ignore/Pos)? "
        inp = sys.stdin.readline().upper()
        pos = -1
        if inp[0] == "Y":
            model[classifi] = [i + j for i, j in zip(model[classifi], features)]
        elif inp[0] == "N":
            model[classifi] = [i - j for i, j in zip(model[classifi], features)]
        elif inp[0] == "P":
            try:
                print "Position? ",
                _ = sys.stdin.readline()
                if _[0] == "$":
                    pos = int(_[1:], 16)
                else:
                    pos = int(_)
                pos -= sid.loadAddress()
            except ValueError:
                pass
        else:
            print "Ignoring..."
    return model


def expert_system(codename, files, retries):
    """
    Start the expert system learning.

    Each file is investigated and the model is updated.

    @param codename: name of the model file
    @param file: list of file names
    @param retries: number of retries per file
    @return: model file
    """
    try:
        model = json.load(file(codename))
    except IOError:
        model = {
            "code" : [0] * 256,
            "data" : [0] * 256
            }
    for fname in files:
        decision(model, fname, retries)
    json.dump(model, file(codename, "w"))
    return model


def main():
    """
    Main function.
    """
    if len(sys.argv) < 2:
        data = [ord(_) for _ in sys.stdin.read() + "\0\0"]
        disassembly = disassemble(data, 0, 0, len(data) - 2)
        disassembly = ["$%04X  %-10s %s" % (line[0], line[1], line[2]) for line in disassembly]
        try:
            print("\n".join(disassembly))
        except IOError:
            pass
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("--model", "-m", help="code analyser model", default="CODE_MODEL.json", required=False)
        parser.add_argument("--tries", "-t", help="how many retries", default=5, required=False, type=int)
        parser.add_argument("--mop", "-M", help="mode of operation", required=True)
        parser.add_argument("files", nargs='+')
        cliargs = parser.parse_args()
        if cliargs.mop == "disasm":
            for fname in cliargs.files:
                print fname
                classifications = dis_file(fname, cliargs.model)
                classifications = [{"code":"3", "data":"7"}[i] for i in classifications]
                with file("/tmp/" + os.path.basename(fname) + ".pgm", "w") as out:
                    width = int(math.sqrt(len(classifications)))
                    classifications.extend(["9"] * width)
                    out.write("P2\n#%s\n%d %d\n9\n" % (fname, width, width + 1))
                    for pos in range(0, len(classifications) - width, width):
                        out.write(" ".join(classifications[pos:pos + width]))
                        out.write("\n")
        elif cliargs.mop == "learn":
            expert_system(cliargs.model, cliargs.files, cliargs.tries)
        else:
            raise RuntimeError("Unknown mode of operation: " + cliargs.mop)

if __name__ == "__main__":
    main()
