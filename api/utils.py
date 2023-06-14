def get_parity(blocks):
    parity = bytearray(blocks[0])
    for i in range(1, len(blocks)):
        for j in range(len(parity)):
            parity[j] ^= blocks[i][j]

    return bytes(parity)