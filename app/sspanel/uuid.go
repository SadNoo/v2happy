package sspanel

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
)

var dnsNamespaceUUID = []byte{
	0x6b, 0xa7, 0xb8, 0x10,
	0x9d, 0xad,
	0x11, 0xd1,
	0x80, 0xb4,
	0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
}

func uuid3DNS(name string) string {
	h := md5.New()
	_, _ = h.Write(dnsNamespaceUUID)
	_, _ = h.Write([]byte(name))
	sum := h.Sum(nil)
	sum[6] = (sum[6] & 0x0f) | 0x30
	sum[8] = (sum[8] & 0x3f) | 0x80
	return formatUUID(sum)
}

func userUUID(userID int, passwd string) string {
	return uuid3DNS(strconv.Itoa(userID) + "|" + passwd)
}

func formatUUID(b []byte) string {
	hexed := hex.EncodeToString(b)
	return hexed[0:8] + "-" + hexed[8:12] + "-" + hexed[12:16] + "-" + hexed[16:20] + "-" + hexed[20:32]
}
