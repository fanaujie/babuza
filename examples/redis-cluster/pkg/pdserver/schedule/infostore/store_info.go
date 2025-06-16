package infostore

type StoreInfo struct {
	storeID     uint64
	leaderCount uint64
}

func CreateStoreInfo(storeID uint64, leaderCount uint64) StoreInfo {
	return StoreInfo{
		storeID:     storeID,
		leaderCount: leaderCount,
	}
}

func (s *StoreInfo) StoreID() uint64 {
	return s.storeID
}

func (s *StoreInfo) LeaderCount() uint64 {
	return s.leaderCount
}
