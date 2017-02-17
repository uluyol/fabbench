package fnv

func Hash64(v int64) int64 {
	const fnvOffsetBasis64 = 0xCBF29CE484222325
	const fnvPrime64 = 1099511628211

	var hashVal uint64 = fnvOffsetBasis64
	for i := 0; i < 8; i++ {
		octet := uint64(v) & 0x00ff
		v = v >> 8

		hashVal ^= octet
		hashVal *= fnvPrime64
	}
	ret := int64(hashVal)
	if ret < 0 {
		return -ret
	}
	return ret
}
