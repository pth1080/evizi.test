def KUniqueCharacters(strParam: str):
    k = int(strParam[0])
    strParam = strParam[1:]
    rs = {}
    for i in range(0, len(strParam)):
        count = 0
        d = ''
        for j in strParam[i:]:
            if j not in d:
                if count < k:
                    count += 1
                    d += j
                else:
                    break
            else:
                d += j

        rs[len(d)] = d
    return rs[max(rs, key=int)]


print(KUniqueCharacters('2aabbcbbbadef'))
