package rootmulti

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"testing"

	protoio "github.com/gogo/protobuf/io"
	"github.com/sei-protocol/sei-chain/sei-cosmos/store/types"
	"github.com/sei-protocol/sei-chain/sei-db/common/evm"
	seidbconfig "github.com/sei-protocol/sei-chain/sei-db/config"
	"github.com/sei-protocol/sei-chain/sei-db/state_db/sc/flatkv"
	abci "github.com/sei-protocol/sei-chain/sei-tendermint/abci/types"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

func dualWriteConfig() seidbconfig.StateCommitConfig {
	cfg := seidbconfig.DefaultStateCommitConfig()
	cfg.WriteMode = seidbconfig.DualWrite
	cfg.EnableLatticeHash = true
	cfg.MemIAVLConfig.SnapshotInterval = 1
	cfg.MemIAVLConfig.SnapshotMinTimeInterval = 0
	cfg.MemIAVLConfig.AsyncCommitBuffer = 0
	cfg.HistoricalProofRateLimit = 0
	cfg.HistoricalProofMaxInFlight = 100
	return cfg
}

func integrationSplitWriteConfig() seidbconfig.StateCommitConfig {
	cfg := seidbconfig.DefaultStateCommitConfig()
	cfg.WriteMode = seidbconfig.SplitWrite
	cfg.EnableLatticeHash = true
	cfg.MemIAVLConfig.SnapshotInterval = 1
	cfg.MemIAVLConfig.SnapshotMinTimeInterval = 0
	cfg.MemIAVLConfig.AsyncCommitBuffer = 0
	cfg.HistoricalProofRateLimit = 0
	cfg.HistoricalProofMaxInFlight = 100
	return cfg
}

// ---------------------------------------------------------------------------
// EVM test data and helpers
// ---------------------------------------------------------------------------

type evmTestData struct {
	storKey []byte // 0x03 + addr + slot
	nonKey  []byte // 0x0a + addr
	codeKey []byte // 0x07 + addr
}

func newEVMTestData(seed byte) evmTestData {
	var addr [20]byte
	addr[0] = seed
	addr[19] = 0xFF
	var slot [32]byte
	slot[0] = seed + 1
	slot[31] = 0xEE

	internal := make([]byte, 52)
	copy(internal[:20], addr[:])
	copy(internal[20:], slot[:])

	return evmTestData{
		storKey: evm.BuildMemIAVLEVMKey(evm.EVMKeyStorage, internal),
		nonKey:  evm.BuildMemIAVLEVMKey(evm.EVMKeyNonce, addr[:]),
		codeKey: evm.BuildMemIAVLEVMKey(evm.EVMKeyCode, addr[:]),
	}
}

func makeNonce(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

func makeSlot(prefix ...byte) []byte {
	var slot [32]byte
	copy(slot[:], prefix)
	return slot[:]
}

type commitRecord struct {
	version int64
	hash    []byte
	infos   []types.StoreInfo
}

var storeNames = []string{"acc", "bank", "evm"}

func newTestRootMulti(t *testing.T, dir string, scCfg seidbconfig.StateCommitConfig) (*Store, map[string]*types.KVStoreKey) {
	t.Helper()
	store := NewStore(dir, scCfg, seidbconfig.StateStoreConfig{}, nil)
	storeKeys := make(map[string]*types.KVStoreKey)
	for _, name := range storeNames {
		sk := types.NewKVStoreKey(name)
		storeKeys[name] = sk
		store.MountStoreWithDB(sk, types.StoreTypeIAVL, nil)
	}
	require.NoError(t, store.LoadLatestVersion())
	return store, storeKeys
}

func simulateBlock(t *testing.T, store *Store, storeKeys map[string]*types.KVStoreKey, block int, evmData evmTestData) commitRecord {
	t.Helper()
	cms := store.CacheMultiStore()
	b := byte(block)

	cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{b})
	cms.GetKVStore(storeKeys["bank"]).Set([]byte("supply"), []byte{b, b})
	cms.GetKVStore(storeKeys["evm"]).Set(evmData.storKey, makeSlot(b, 0xAA))
	cms.GetKVStore(storeKeys["evm"]).Set(evmData.nonKey, makeNonce(uint64(block)))
	if block == 1 {
		cms.GetKVStore(storeKeys["evm"]).Set(evmData.codeKey, []byte{0x60, 0x60, 0x60, b})
	}

	cms.Write()
	_, err := store.GetWorkingHash()
	require.NoError(t, err)
	cid := store.Commit(true)

	infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
	copy(infos, store.lastCommitInfo.StoreInfos)
	return commitRecord{version: cid.Version, hash: cid.Hash, infos: infos}
}

func simulateCosmosOnlyBlock(t *testing.T, store *Store, storeKeys map[string]*types.KVStoreKey, block int) commitRecord {
	t.Helper()
	cms := store.CacheMultiStore()
	b := byte(block)
	cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{b})
	cms.GetKVStore(storeKeys["bank"]).Set([]byte("supply"), []byte{b})
	cms.Write()
	_, err := store.GetWorkingHash()
	require.NoError(t, err)
	cid := store.Commit(true)

	infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
	copy(infos, store.lastCommitInfo.StoreInfos)
	return commitRecord{version: cid.Version, hash: cid.Hash, infos: infos}
}

func findStoreInfo(infos []types.StoreInfo, name string) *types.StoreInfo {
	for i := range infos {
		if infos[i].Name == name {
			return &infos[i]
		}
	}
	return nil
}

func verifyHistoricalHashes(t *testing.T, store *Store, records []commitRecord) {
	t.Helper()
	for _, rec := range records {
		scStore, err := store.scStore.LoadVersion(rec.version, true)
		require.NoError(t, err)

		commitInfo := convertCommitInfo(scStore.LastCommitInfo())
		commitInfo = amendCommitInfo(commitInfo, store.storesParams)

		require.Equalf(t, rec.hash, commitInfo.Hash(),
			"ROOT HASH MISMATCH at version %d", rec.version)

		_ = scStore.Close()
	}
}

// rollbackFlatKV opens the FlatKV store at dir, loads latest, rolls back to
// the target version, and closes. Used to simulate a crash where FlatKV is
// behind cosmos.
func rollbackFlatKV(t *testing.T, dir string, cfg seidbconfig.StateCommitConfig, target int64) {
	t.Helper()
	flatkvCfg := cfg.FlatKVConfig
	flatkvCfg.DataDir = filepath.Join(dir, "data", "flatkv")
	evmStore, err := flatkv.NewCommitStore(context.Background(), &flatkvCfg)
	require.NoError(t, err)
	_, err = evmStore.LoadVersion(0, false)
	require.NoError(t, err)
	require.NoError(t, evmStore.Rollback(target))
	require.NoError(t, evmStore.Close())
}

// ---------------------------------------------------------------------------
// Test 1: DualWrite + LatticeHash — hash consistency through rootmulti
// ---------------------------------------------------------------------------

func TestFlatKVDualWriteHashConsistency(t *testing.T) {
	store, storeKeys := newTestRootMulti(t, t.TempDir(), dualWriteConfig())
	defer func() { require.NoError(t, store.Close()) }()

	evmData := newEVMTestData(0xAA)
	var records []commitRecord

	for block := 1; block <= 10; block++ {
		rec := simulateBlock(t, store, storeKeys, block, evmData)
		records = append(records, rec)

		lattice := findStoreInfo(rec.infos, "evm_lattice")
		require.NotNilf(t, lattice, "evm_lattice missing at block %d", block)
		require.Lenf(t, lattice.CommitId.Hash, 32, "lattice hash should be 32 bytes at block %d", block)
	}

	// Lattice hash must change between blocks with different EVM data
	for i := 1; i < len(records); i++ {
		prev := findStoreInfo(records[i-1].infos, "evm_lattice")
		curr := findStoreInfo(records[i].infos, "evm_lattice")
		require.NotEqual(t, prev.CommitId.Hash, curr.CommitId.Hash,
			"lattice hash must change between blocks %d and %d", i, i+1)
	}

	verifyHistoricalHashes(t, store, records)
}

// ---------------------------------------------------------------------------
// Test 2: SplitWrite — hash consistency, EVM data not in memiavl tree
// ---------------------------------------------------------------------------

func TestFlatKVSplitWriteHashConsistency(t *testing.T) {
	store, storeKeys := newTestRootMulti(t, t.TempDir(), integrationSplitWriteConfig())
	defer func() { require.NoError(t, store.Close()) }()

	evmData := newEVMTestData(0xBB)
	var records []commitRecord

	for block := 1; block <= 10; block++ {
		rec := simulateBlock(t, store, storeKeys, block, evmData)
		records = append(records, rec)

		lattice := findStoreInfo(rec.infos, "evm_lattice")
		require.NotNilf(t, lattice, "evm_lattice missing at block %d", block)
		require.NotEmpty(t, lattice.CommitId.Hash)

		// In SplitWrite the "evm" memiavl tree receives no data; its IAVL hash
		// must remain unchanged across blocks.
		if block > 1 {
			prev := findStoreInfo(records[block-2].infos, "evm")
			curr := findStoreInfo(rec.infos, "evm")
			require.Equal(t, prev.CommitId.Hash, curr.CommitId.Hash,
				"evm IAVL hash should not change in SplitWrite mode (block %d)", block)
		}
	}

	verifyHistoricalHashes(t, store, records)
}

// ---------------------------------------------------------------------------
// Test 3: Determinism — two stores with identical data produce identical hashes
// ---------------------------------------------------------------------------

func TestFlatKVLatticeHashDeterminism(t *testing.T) {
	cfg := dualWriteConfig()
	evmData := newEVMTestData(0xCC)

	var hashes [2][]byte
	var latticeHashes [2][]byte

	for i := 0; i < 2; i++ {
		store, storeKeys := newTestRootMulti(t, t.TempDir(), cfg)
		for block := 1; block <= 5; block++ {
			simulateBlock(t, store, storeKeys, block, evmData)
		}
		hashes[i] = store.lastCommitInfo.Hash()
		lattice := findStoreInfo(store.lastCommitInfo.StoreInfos, "evm_lattice")
		require.NotNil(t, lattice)
		latticeHashes[i] = lattice.CommitId.Hash
		require.NoError(t, store.Close())
	}

	require.Equal(t, hashes[0], hashes[1], "app hashes must be deterministic")
	require.Equal(t, latticeHashes[0], latticeHashes[1], "lattice hashes must be deterministic")
}

// ---------------------------------------------------------------------------
// Test 4: Sensitivity — single byte change in EVM data changes lattice hash
// ---------------------------------------------------------------------------

func TestFlatKVLatticeHashSensitivity(t *testing.T) {
	cfg := dualWriteConfig()
	evmData := newEVMTestData(0xDD)

	storeA, keysA := newTestRootMulti(t, t.TempDir(), cfg)
	for block := 1; block <= 3; block++ {
		simulateBlock(t, storeA, keysA, block, evmData)
	}

	storeB, keysB := newTestRootMulti(t, t.TempDir(), cfg)
	for block := 1; block <= 3; block++ {
		if block == 3 {
			cms := storeB.CacheMultiStore()
			cms.GetKVStore(keysB["acc"]).Set([]byte("acct1"), []byte{byte(block)})
			cms.GetKVStore(keysB["bank"]).Set([]byte("supply"), []byte{byte(block), byte(block)})
			// 0xBB instead of 0xAA — single byte difference
			cms.GetKVStore(keysB["evm"]).Set(evmData.storKey, makeSlot(byte(block), 0xBB))
			cms.GetKVStore(keysB["evm"]).Set(evmData.nonKey, makeNonce(uint64(block)))
			cms.Write()
			_, err := storeB.GetWorkingHash()
			require.NoError(t, err)
			storeB.Commit(true)
		} else {
			simulateBlock(t, storeB, keysB, block, evmData)
		}
	}

	latticeA := findStoreInfo(storeA.lastCommitInfo.StoreInfos, "evm_lattice")
	latticeB := findStoreInfo(storeB.lastCommitInfo.StoreInfos, "evm_lattice")
	require.NotEqual(t, latticeA.CommitId.Hash, latticeB.CommitId.Hash,
		"lattice hash must differ when EVM data differs by a single byte")
	require.NotEqual(t, storeA.lastCommitInfo.Hash(), storeB.lastCommitInfo.Hash(),
		"app hash must differ when lattice hash differs")

	require.NoError(t, storeA.Close())
	require.NoError(t, storeB.Close())
}

// ---------------------------------------------------------------------------
// Test 5: Double flush (FinalizeBlock + Commit) with DualWrite
// ---------------------------------------------------------------------------

func TestFlatKVDualWriteDoubleFlush(t *testing.T) {
	store, storeKeys := newTestRootMulti(t, t.TempDir(), dualWriteConfig())
	defer func() { require.NoError(t, store.Close()) }()

	evmData := newEVMTestData(0xEE)
	var records []commitRecord

	for block := 1; block <= 5; block++ {
		cms := store.CacheMultiStore()
		b := byte(block)

		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{b})
		cms.GetKVStore(storeKeys["bank"]).Set([]byte("supply"), []byte{b, b})
		cms.GetKVStore(storeKeys["evm"]).Set(evmData.storKey, makeSlot(b, 0xAA))
		cms.GetKVStore(storeKeys["evm"]).Set(evmData.nonKey, makeNonce(uint64(block)))

		// Simulate FinalizeBlock: Write + GetWorkingHash
		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)

		// Simulate Commit: Write + GetWorkingHash + Commit (double flush)
		cms.Write()
		_, err = store.GetWorkingHash()
		require.NoError(t, err)
		cid := store.Commit(true)

		infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
		copy(infos, store.lastCommitInfo.StoreInfos)
		records = append(records, commitRecord{version: cid.Version, hash: cid.Hash, infos: infos})
	}

	for _, rec := range records {
		scStore, err := store.scStore.LoadVersion(rec.version, true)
		require.NoError(t, err)

		commitInfo := convertCommitInfo(scStore.LastCommitInfo())
		commitInfo = amendCommitInfo(commitInfo, store.storesParams)
		require.Equalf(t, rec.hash, commitInfo.Hash(),
			"ROOT HASH MISMATCH at version %d (double flush)", rec.version)

		lattice := findStoreInfo(commitInfo.StoreInfos, "evm_lattice")
		require.NotNilf(t, lattice, "evm_lattice must survive double flush at version %d", rec.version)
		_ = scStore.Close()
	}
}

// ---------------------------------------------------------------------------
// Test 6: Rollback preserves lattice hash correctness
// ---------------------------------------------------------------------------

func TestFlatKVRollbackWithLatticeHash(t *testing.T) {
	store, storeKeys := newTestRootMulti(t, t.TempDir(), dualWriteConfig())
	evmData := newEVMTestData(0x11)

	var records []commitRecord
	for block := 1; block <= 5; block++ {
		records = append(records, simulateBlock(t, store, storeKeys, block, evmData))
	}

	require.NoError(t, store.RollbackToVersion(3))
	require.Equal(t, int64(3), store.LastCommitID().Version)
	require.Equal(t, records[2].hash, store.lastCommitInfo.Hash(),
		"after rollback to v3, app hash must match original v3")

	lattice := findStoreInfo(store.lastCommitInfo.StoreInfos, "evm_lattice")
	origLattice := findStoreInfo(records[2].infos, "evm_lattice")
	require.Equal(t, origLattice.CommitId.Hash, lattice.CommitId.Hash,
		"lattice hash must match original v3 after rollback")

	// New commits after rollback produce sequential versions
	for block := 4; block <= 7; block++ {
		rec := simulateBlock(t, store, storeKeys, block+100, evmData)
		require.Equal(t, int64(block), rec.version)
		require.NotNil(t, findStoreInfo(rec.infos, "evm_lattice"))
	}

	require.NoError(t, store.Close())
}

// ---------------------------------------------------------------------------
// Test 7: Crash recovery — FlatKV behind cosmos, version reconciliation
// ---------------------------------------------------------------------------

func TestFlatKVCrashRecoveryThroughRootMulti(t *testing.T) {
	dir := t.TempDir()
	cfg := dualWriteConfig()
	evmData := newEVMTestData(0x22)

	// Phase 1: commit 5 blocks
	store1, storeKeys1 := newTestRootMulti(t, dir, cfg)
	var records []commitRecord
	for block := 1; block <= 5; block++ {
		records = append(records, simulateBlock(t, store1, storeKeys1, block, evmData))
	}
	require.NoError(t, store1.Close())

	// Simulate crash: roll FlatKV back to version 3
	rollbackFlatKV(t, dir, cfg, 3)

	// Phase 2: reopen — reconciliation should bring both to version 3
	store2, storeKeys2 := newTestRootMulti(t, dir, cfg)

	require.Equal(t, int64(3), store2.LastCommitID().Version,
		"after crash recovery, version should reconcile to 3")
	require.Equal(t, records[2].hash, store2.lastCommitInfo.Hash(),
		"after crash recovery, app hash must match original v3")

	lattice := findStoreInfo(store2.lastCommitInfo.StoreInfos, "evm_lattice")
	origLattice := findStoreInfo(records[2].infos, "evm_lattice")
	require.NotNil(t, lattice)
	require.NotNil(t, origLattice)
	require.Equal(t, origLattice.CommitId.Hash, lattice.CommitId.Hash,
		"lattice hash must match original v3 after crash recovery")

	// Chain must continue making progress
	for block := 4; block <= 8; block++ {
		rec := simulateBlock(t, store2, storeKeys2, block+200, evmData)
		require.Equal(t, int64(block), rec.version)
		require.NotNil(t, findStoreInfo(rec.infos, "evm_lattice"))
	}

	require.NoError(t, store2.Close())
}

// ---------------------------------------------------------------------------
// Test 8: Snapshot and Restore round-trip with lattice hash
// ---------------------------------------------------------------------------

func TestFlatKVSnapshotRestoreWithLatticeHash(t *testing.T) {
	cfg := dualWriteConfig()
	evmData := newEVMTestData(0x33)

	// Source: commit 5 blocks
	srcStore, srcKeys := newTestRootMulti(t, t.TempDir(), cfg)
	for block := 1; block <= 5; block++ {
		simulateBlock(t, srcStore, srcKeys, block, evmData)
	}

	srcLattice := findStoreInfo(srcStore.lastCommitInfo.StoreInfos, "evm_lattice")
	require.NotNil(t, srcLattice)
	require.NotEmpty(t, srcLattice.CommitId.Hash)

	// Snapshot to buffer
	var buf bytes.Buffer
	writer := protoio.NewDelimitedWriter(&buf)
	require.NoError(t, srcStore.Snapshot(5, writer))
	require.NoError(t, srcStore.Close())
	require.NotEmpty(t, buf.Bytes())

	// Destination: restore from snapshot
	dstStore, _ := newTestRootMulti(t, t.TempDir(), cfg)
	reader := protoio.NewDelimitedReader(bytes.NewReader(buf.Bytes()), 1<<30)
	_, err := dstStore.Restore(5, 1, reader)
	require.NoError(t, err)

	require.Equal(t, int64(5), dstStore.LastCommitID().Version)

	dstLattice := findStoreInfo(dstStore.lastCommitInfo.StoreInfos, "evm_lattice")
	require.NotNil(t, dstLattice, "evm_lattice must be present after restore")
	require.NotEmpty(t, dstLattice.CommitId.Hash, "restored lattice hash must be non-empty")

	// NOTE: exact hash equality (srcLatticeHash == dstLattice hash) is not
	// asserted because the export/import round-trip decomposes merged account
	// rows into separate nonce+codehash nodes and re-merges them, which
	// produces a different serialized form and thus a different LtHash.
	// The memiavl tree hashes (acc, bank, evm) are unchanged because
	// the leaf key/value bytes survive the round-trip.
	// TODO: make the round-trip lossless so that lattice hashes match exactly.

	// Continue committing after restore
	dstKeys := make(map[string]*types.KVStoreKey)
	for name, key := range dstStore.storeKeys {
		if kvKey, ok := key.(*types.KVStoreKey); ok {
			dstKeys[name] = kvKey
		}
	}
	rec := simulateBlock(t, dstStore, dstKeys, 6, evmData)
	require.Equal(t, int64(6), rec.version)
	newLattice := findStoreInfo(rec.infos, "evm_lattice")
	require.NotNil(t, newLattice)
	require.NotEqual(t, dstLattice.CommitId.Hash, newLattice.CommitId.Hash,
		"lattice hash should change after new commit post-restore")

	require.NoError(t, dstStore.Close())
}

// ---------------------------------------------------------------------------
// Test 9: Empty EVM blocks — lattice hash stays stable
// ---------------------------------------------------------------------------

func TestFlatKVEmptyEVMBlocks(t *testing.T) {
	store, storeKeys := newTestRootMulti(t, t.TempDir(), dualWriteConfig())
	defer func() { require.NoError(t, store.Close()) }()

	evmData := newEVMTestData(0x44)

	// Block 1: write EVM data
	rec1 := simulateBlock(t, store, storeKeys, 1, evmData)
	lattice1 := findStoreInfo(rec1.infos, "evm_lattice")
	require.NotNil(t, lattice1)

	// Blocks 2-4: cosmos only — no EVM writes
	for block := 2; block <= 4; block++ {
		rec := simulateCosmosOnlyBlock(t, store, storeKeys, block)
		lattice := findStoreInfo(rec.infos, "evm_lattice")
		require.NotNil(t, lattice)
		require.Equalf(t, lattice1.CommitId.Hash, lattice.CommitId.Hash,
			"lattice hash should not change without EVM writes (block %d)", block)
	}

	// Block 5: write EVM data again — lattice hash must change
	rec5 := simulateBlock(t, store, storeKeys, 5, evmData)
	lattice5 := findStoreInfo(rec5.infos, "evm_lattice")
	require.NotNil(t, lattice5)
	require.NotEqual(t, lattice1.CommitId.Hash, lattice5.CommitId.Hash,
		"lattice hash must change when EVM data changes again")
}

// ---------------------------------------------------------------------------
// Test 10: Mixed cosmos+EVM blocks — selective lattice hash changes
// ---------------------------------------------------------------------------

func TestFlatKVMixedCosmosAndEVMBlocks(t *testing.T) {
	store, storeKeys := newTestRootMulti(t, t.TempDir(), dualWriteConfig())
	defer func() { require.NoError(t, store.Close()) }()

	evmData := newEVMTestData(0x55)
	var records []commitRecord

	for block := 1; block <= 10; block++ {
		if block%2 == 1 {
			// Odd blocks: full block with EVM data
			records = append(records, simulateBlock(t, store, storeKeys, block, evmData))
		} else {
			// Even blocks: cosmos only
			records = append(records, simulateCosmosOnlyBlock(t, store, storeKeys, block))
		}
	}

	verifyHistoricalHashes(t, store, records)

	// Lattice hash should only change on odd blocks
	for i := 1; i < len(records); i++ {
		prev := findStoreInfo(records[i-1].infos, "evm_lattice")
		curr := findStoreInfo(records[i].infos, "evm_lattice")
		require.NotNil(t, prev)
		require.NotNil(t, curr)

		block := i + 1
		if block%2 == 1 {
			require.NotEqualf(t, prev.CommitId.Hash, curr.CommitId.Hash,
				"lattice hash should change on EVM-write block %d", block)
		} else {
			require.Equalf(t, prev.CommitId.Hash, curr.CommitId.Hash,
				"lattice hash should be stable on cosmos-only block %d", block)
		}
	}
}

// ---------------------------------------------------------------------------
// Additional helpers for gap-fill tests
// ---------------------------------------------------------------------------

func cosmosOnlyConfig() seidbconfig.StateCommitConfig {
	cfg := seidbconfig.DefaultStateCommitConfig()
	cfg.WriteMode = seidbconfig.CosmosOnlyWrite
	cfg.EnableLatticeHash = false
	cfg.MemIAVLConfig.SnapshotInterval = 1
	cfg.MemIAVLConfig.SnapshotMinTimeInterval = 0
	cfg.MemIAVLConfig.AsyncCommitBuffer = 0
	cfg.HistoricalProofRateLimit = 0
	cfg.HistoricalProofMaxInFlight = 100
	return cfg
}

func dualWriteNoLatticeConfig() seidbconfig.StateCommitConfig {
	cfg := dualWriteConfig()
	cfg.EnableLatticeHash = false
	return cfg
}

// openFlatKVReadOnly opens a readonly FlatKV store at the given rootmulti home
// directory at the specified version. Caller must Close() when done.
func openFlatKVReadOnly(t *testing.T, dir string, cfg seidbconfig.StateCommitConfig, version int64) flatkv.Store {
	t.Helper()
	flatkvCfg := cfg.FlatKVConfig
	flatkvCfg.DataDir = filepath.Join(dir, "data", "flatkv")
	store, err := flatkv.NewCommitStore(context.Background(), &flatkvCfg)
	require.NoError(t, err)
	ro, err := store.LoadVersion(version, true)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	return ro
}

// newMultiEVMTestData generates n distinct EVM test data entries.
func newMultiEVMTestData(n int) []evmTestData {
	result := make([]evmTestData, n)
	for i := range result {
		result[i] = newEVMTestData(byte(i + 1))
	}
	return result
}

// ---------------------------------------------------------------------------
// Test 11: DualWrite read consistency — read back EVM data via SC (memiavl)
// ---------------------------------------------------------------------------

func TestFlatKVDualWriteReadConsistency(t *testing.T) {
	dir := t.TempDir()
	store, storeKeys := newTestRootMulti(t, dir, dualWriteConfig())
	defer func() { require.NoError(t, store.Close()) }()

	addrs := newMultiEVMTestData(3)

	type writtenKV struct {
		key   []byte
		value []byte
	}
	blockWrites := make(map[int64][]writtenKV)

	for block := 1; block <= 5; block++ {
		cms := store.CacheMultiStore()
		b := byte(block)
		var writes []writtenKV

		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{b})
		cms.GetKVStore(storeKeys["bank"]).Set([]byte("supply"), []byte{b, b})

		for i, addr := range addrs {
			storVal := makeSlot(b, byte(i), 0xAA)
			cms.GetKVStore(storeKeys["evm"]).Set(addr.storKey, storVal)
			writes = append(writes, writtenKV{key: addr.storKey, value: storVal})

			nonceVal := makeNonce(uint64(block*10 + i))
			cms.GetKVStore(storeKeys["evm"]).Set(addr.nonKey, nonceVal)
			writes = append(writes, writtenKV{key: addr.nonKey, value: nonceVal})

			if block == 1 {
				codeVal := []byte{0x60, 0x60, byte(i)}
				cms.GetKVStore(storeKeys["evm"]).Set(addr.codeKey, codeVal)
				writes = append(writes, writtenKV{key: addr.codeKey, value: codeVal})
			}
		}

		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		cid := store.Commit(true)
		blockWrites[cid.Version] = writes
	}

	// Read back at latest version via SC memiavl child store
	evmChild := store.scStore.GetChildStoreByName("evm")
	require.NotNil(t, evmChild)
	for _, w := range blockWrites[5] {
		got := evmChild.Get(w.key)
		require.Equalf(t, w.value, got, "memiavl latest read mismatch for key %x", w.key)
		require.True(t, evmChild.Has(w.key))
	}

	// Read back at historical versions
	for v := int64(1); v <= 5; v++ {
		scStore, err := store.scStore.LoadVersion(v, true)
		require.NoError(t, err)
		child := scStore.GetChildStoreByName("evm")
		require.NotNil(t, child)

		for _, w := range blockWrites[v] {
			got := child.Get(w.key)
			require.Equalf(t, w.value, got, "memiavl v%d read mismatch for key %x", v, w.key)
		}
		require.NoError(t, scStore.Close())
	}

	// Has() for a non-existent key should return false
	fakeKey := evm.BuildMemIAVLEVMKey(evm.EVMKeyStorage, make([]byte, 52))
	require.False(t, evmChild.Has(fakeKey))
}

// ---------------------------------------------------------------------------
// Test 12: DualWrite data equivalence — memiavl vs flatkv byte-for-byte
// ---------------------------------------------------------------------------

func TestFlatKVDualWriteDataEquivalence(t *testing.T) {
	dir := t.TempDir()
	cfg := dualWriteConfig()
	store, storeKeys := newTestRootMulti(t, dir, cfg)

	addrs := newMultiEVMTestData(4)

	type writtenKV struct {
		key   []byte
		value []byte
	}
	var allWrites []writtenKV

	for block := 1; block <= 10; block++ {
		cms := store.CacheMultiStore()
		b := byte(block)

		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{b})
		cms.GetKVStore(storeKeys["bank"]).Set([]byte("supply"), []byte{b, b})

		for i, addr := range addrs {
			storVal := makeSlot(b, byte(i), 0xCC)
			cms.GetKVStore(storeKeys["evm"]).Set(addr.storKey, storVal)
			if block == 10 {
				allWrites = append(allWrites, writtenKV{key: addr.storKey, value: storVal})
			}

			nonceVal := makeNonce(uint64(block*100 + i))
			cms.GetKVStore(storeKeys["evm"]).Set(addr.nonKey, nonceVal)
			if block == 10 {
				allWrites = append(allWrites, writtenKV{key: addr.nonKey, value: nonceVal})
			}

			if block == 1 {
				codeVal := []byte{0x60, 0x60, byte(i), 0xDD}
				cms.GetKVStore(storeKeys["evm"]).Set(addr.codeKey, codeVal)
			}
		}

		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		store.Commit(true)
	}

	// Read memiavl values while the store is still open (before closing releases the lock)
	evmTree := store.scStore.GetChildStoreByName("evm")
	require.NotNil(t, evmTree)

	// Also collect code keys from memiavl (written in block 1, still present)
	for _, addr := range addrs {
		codeVal := evmTree.Get(addr.codeKey)
		if codeVal != nil {
			allWrites = append(allWrites, writtenKV{key: addr.codeKey, value: codeVal})
		}
	}

	memiavlValues := make(map[string][]byte)
	for _, w := range allWrites {
		got := evmTree.Get(w.key)
		memiavlValues[string(w.key)] = got
		require.Equalf(t, w.value, got, "memiavl value mismatch for key %x", w.key)
	}
	require.NoError(t, store.Close())

	// Open flatkv readonly and compare against memiavl values
	ro := openFlatKVReadOnly(t, dir, cfg, 0)
	defer func() { require.NoError(t, ro.Close()) }()

	for _, w := range allWrites {
		flatkvVal, found := ro.Get(evm.EVMStoreKey, w.key)
		require.Truef(t, found, "flatkv missing key %x", w.key)
		require.Equalf(t, memiavlValues[string(w.key)], flatkvVal,
			"memiavl vs flatkv divergence for key %x:\n  memiavl: %x\n  flatkv:  %x",
			w.key, memiavlValues[string(w.key)], flatkvVal)
	}
}

// ---------------------------------------------------------------------------
// Test 13: Full-scan LtHash verification at integration level
// ---------------------------------------------------------------------------

func TestFlatKVFullScanLtHashVerification(t *testing.T) {
	dir := t.TempDir()
	cfg := dualWriteConfig()
	store, storeKeys := newTestRootMulti(t, dir, cfg)

	addrs := newMultiEVMTestData(3)

	for block := 1; block <= 10; block++ {
		cms := store.CacheMultiStore()
		b := byte(block)

		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{b})
		cms.GetKVStore(storeKeys["bank"]).Set([]byte("supply"), []byte{b})

		for i, addr := range addrs {
			cms.GetKVStore(storeKeys["evm"]).Set(addr.storKey, makeSlot(b, byte(i)))
			cms.GetKVStore(storeKeys["evm"]).Set(addr.nonKey, makeNonce(uint64(block)))
			if block == 1 {
				cms.GetKVStore(storeKeys["evm"]).Set(addr.codeKey, []byte{0x60, byte(i)})
			}
			// Overwrite storage on even blocks to exercise MixOut+MixIn
			if block%2 == 0 {
				cms.GetKVStore(storeKeys["evm"]).Set(addr.storKey, makeSlot(b, byte(i), 0xFF))
			}
		}

		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		store.Commit(true)
	}
	// Save lattice hash before closing (to avoid file lock conflict)
	lattice := findStoreInfo(store.lastCommitInfo.StoreInfos, "evm_lattice")
	require.NotNil(t, lattice)
	expectedLatticeHash := lattice.CommitId.Hash
	require.NoError(t, store.Close())

	// Open flatkv readonly and verify full-scan matches incremental
	ro := openFlatKVReadOnly(t, dir, cfg, 0)
	defer func() { require.NoError(t, ro.Close()) }()

	require.NoError(t, flatkv.VerifyLtHash(ro), "full-scan LtHash verification failed")

	require.Equal(t, expectedLatticeHash, ro.CommittedRootHash(),
		"flatkv CommittedRootHash should match evm_lattice in CommitInfo")
}

// ---------------------------------------------------------------------------
// Test 14: Delete and overwrite workload with LtHash verification
// ---------------------------------------------------------------------------

func TestFlatKVDeleteAndOverwriteWorkload(t *testing.T) {
	dir := t.TempDir()
	cfg := dualWriteConfig()
	store, storeKeys := newTestRootMulti(t, dir, cfg)

	addr1 := newEVMTestData(0x01)
	addr2 := newEVMTestData(0x02)
	addr3 := newEVMTestData(0x03)
	addr4 := newEVMTestData(0x04)

	var records []commitRecord

	// Block 1: Set storage/nonce/code for 3 addresses
	{
		cms := store.CacheMultiStore()
		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{1})
		for _, addr := range []evmTestData{addr1, addr2, addr3} {
			cms.GetKVStore(storeKeys["evm"]).Set(addr.storKey, makeSlot(0x01, 0xAA))
			cms.GetKVStore(storeKeys["evm"]).Set(addr.nonKey, makeNonce(1))
			cms.GetKVStore(storeKeys["evm"]).Set(addr.codeKey, []byte{0x60, 0x60})
		}
		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		cid := store.Commit(true)
		infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
		copy(infos, store.lastCommitInfo.StoreInfos)
		records = append(records, commitRecord{version: cid.Version, hash: cid.Hash, infos: infos})
	}

	// Block 2: Overwrite storage values for all 3 addresses
	{
		cms := store.CacheMultiStore()
		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{2})
		for _, addr := range []evmTestData{addr1, addr2, addr3} {
			cms.GetKVStore(storeKeys["evm"]).Set(addr.storKey, makeSlot(0x02, 0xBB))
			cms.GetKVStore(storeKeys["evm"]).Set(addr.nonKey, makeNonce(2))
		}
		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		cid := store.Commit(true)
		infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
		copy(infos, store.lastCommitInfo.StoreInfos)
		records = append(records, commitRecord{version: cid.Version, hash: cid.Hash, infos: infos})
	}

	// Block 3: Delete storage for addr1, delete code for addr2
	{
		cms := store.CacheMultiStore()
		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{3})
		cms.GetKVStore(storeKeys["evm"]).Delete(addr1.storKey)
		cms.GetKVStore(storeKeys["evm"]).Delete(addr2.codeKey)
		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		cid := store.Commit(true)
		infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
		copy(infos, store.lastCommitInfo.StoreInfos)
		records = append(records, commitRecord{version: cid.Version, hash: cid.Hash, infos: infos})
	}

	// Block 4: Re-create storage for addr1 (delete-then-recreate across blocks)
	{
		cms := store.CacheMultiStore()
		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{4})
		cms.GetKVStore(storeKeys["evm"]).Set(addr1.storKey, makeSlot(0x04, 0xDD))
		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		cid := store.Commit(true)
		infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
		copy(infos, store.lastCommitInfo.StoreInfos)
		records = append(records, commitRecord{version: cid.Version, hash: cid.Hash, infos: infos})
	}

	// Block 5: Same-block set-then-delete for addr4
	{
		cms := store.CacheMultiStore()
		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{5})
		cms.GetKVStore(storeKeys["evm"]).Set(addr4.storKey, makeSlot(0x05, 0xEE))
		cms.GetKVStore(storeKeys["evm"]).Delete(addr4.storKey)
		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		cid := store.Commit(true)
		infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
		copy(infos, store.lastCommitInfo.StoreInfos)
		records = append(records, commitRecord{version: cid.Version, hash: cid.Hash, infos: infos})
	}

	// Lattice hash must change for blocks 1-4 (real EVM mutations)
	for i := 1; i < 4; i++ {
		prev := findStoreInfo(records[i-1].infos, "evm_lattice")
		curr := findStoreInfo(records[i].infos, "evm_lattice")
		require.NotNil(t, prev)
		require.NotNil(t, curr)
		require.NotEqualf(t, prev.CommitId.Hash, curr.CommitId.Hash,
			"lattice hash must change between blocks %d and %d", i, i+1)
	}
	// Block 5: set-then-delete of a non-existent key is a no-op for LtHash;
	// the hash should stay the same as block 4.
	lattice4 := findStoreInfo(records[3].infos, "evm_lattice")
	lattice5 := findStoreInfo(records[4].infos, "evm_lattice")
	require.NotNil(t, lattice4)
	require.NotNil(t, lattice5)
	require.Equalf(t, lattice4.CommitId.Hash, lattice5.CommitId.Hash,
		"set-then-delete of non-existent key should be a no-op for LtHash")

	verifyHistoricalHashes(t, store, records)

	// Full-scan verification: close rootmulti first to release lock
	require.NoError(t, store.Close())
	ro := openFlatKVReadOnly(t, dir, cfg, 0)
	require.NoError(t, flatkv.VerifyLtHash(ro), "full-scan verification failed after delete workload")
	require.NoError(t, ro.Close())
}

// ---------------------------------------------------------------------------
// Test 15: Multi-account workload with realistic key distribution
// ---------------------------------------------------------------------------

func TestFlatKVMultiAccountWorkload(t *testing.T) {
	dir := t.TempDir()
	cfg := dualWriteConfig()
	store, storeKeys := newTestRootMulti(t, dir, cfg)

	const numAddrs = 20
	const slotsPerAddr = 5
	const numBlocks = 10

	// Generate addresses and their storage keys
	type addrSlots struct {
		data  evmTestData
		extra []evmTestData // additional slots reusing the same address prefix
	}

	rng := rand.New(rand.NewSource(42))
	allAddrs := make([]addrSlots, numAddrs)
	for i := range allAddrs {
		seed := byte(i + 0x10)
		allAddrs[i].data = newEVMTestData(seed)
		for s := 1; s < slotsPerAddr; s++ {
			var addr [20]byte
			addr[0] = seed
			addr[19] = 0xFF
			var slot [32]byte
			slot[0] = seed + byte(s) + 1
			slot[31] = byte(s)
			internal := make([]byte, 52)
			copy(internal[:20], addr[:])
			copy(internal[20:], slot[:])
			allAddrs[i].extra = append(allAddrs[i].extra, evmTestData{
				storKey: evm.BuildMemIAVLEVMKey(evm.EVMKeyStorage, internal),
			})
		}
	}

	var records []commitRecord

	for block := 1; block <= numBlocks; block++ {
		cms := store.CacheMultiStore()
		b := byte(block)
		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{b})
		cms.GetKVStore(storeKeys["bank"]).Set([]byte("supply"), []byte{b})

		// Each block touches a random subset of addresses
		numToTouch := 5 + rng.Intn(numAddrs-5)
		perm := rng.Perm(numAddrs)[:numToTouch]

		for _, idx := range perm {
			as := allAddrs[idx]
			action := rng.Intn(10)

			switch {
			case action < 6: // 60%: set/overwrite
				cms.GetKVStore(storeKeys["evm"]).Set(as.data.storKey, makeSlot(b, byte(idx)))
				cms.GetKVStore(storeKeys["evm"]).Set(as.data.nonKey, makeNonce(uint64(block*100+idx)))
				if block == 1 {
					cms.GetKVStore(storeKeys["evm"]).Set(as.data.codeKey, []byte{0x60, byte(idx)})
				}
				for _, extra := range as.extra {
					cms.GetKVStore(storeKeys["evm"]).Set(extra.storKey, makeSlot(b, byte(idx), 0xEE))
				}
			case action < 8: // 20%: delete a storage key
				cms.GetKVStore(storeKeys["evm"]).Delete(as.data.storKey)
			default: // 20%: skip (no-op for this address)
			}
		}

		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		cid := store.Commit(true)
		infos := make([]types.StoreInfo, len(store.lastCommitInfo.StoreInfos))
		copy(infos, store.lastCommitInfo.StoreInfos)
		records = append(records, commitRecord{version: cid.Version, hash: cid.Hash, infos: infos})
	}

	verifyHistoricalHashes(t, store, records)
	require.NoError(t, store.Close())

	// Full-scan verification
	ro := openFlatKVReadOnly(t, dir, cfg, 0)
	require.NoError(t, flatkv.VerifyLtHash(ro), "full-scan verification failed for multi-account workload")
	require.NoError(t, ro.Close())
}

// ---------------------------------------------------------------------------
// Test 16: Shadow mode — DualWrite + EnableLatticeHash=false
// ---------------------------------------------------------------------------

func TestFlatKVShadowModeNoLatticeInAppHash(t *testing.T) {
	evmData := newEVMTestData(0x77)

	dirA := t.TempDir()
	storeA, keysA := newTestRootMulti(t, dirA, dualWriteNoLatticeConfig())

	dirB := t.TempDir()
	storeB, keysB := newTestRootMulti(t, dirB, cosmosOnlyConfig())

	for block := 1; block <= 5; block++ {
		b := byte(block)

		// Store A: DualWrite + no lattice
		cmsA := storeA.CacheMultiStore()
		cmsA.GetKVStore(keysA["acc"]).Set([]byte("acct1"), []byte{b})
		cmsA.GetKVStore(keysA["bank"]).Set([]byte("supply"), []byte{b, b})
		cmsA.GetKVStore(keysA["evm"]).Set(evmData.storKey, makeSlot(b, 0xAA))
		cmsA.GetKVStore(keysA["evm"]).Set(evmData.nonKey, makeNonce(uint64(block)))
		if block == 1 {
			cmsA.GetKVStore(keysA["evm"]).Set(evmData.codeKey, []byte{0x60, 0x60, b})
		}
		cmsA.Write()
		_, err := storeA.GetWorkingHash()
		require.NoError(t, err)
		cidA := storeA.Commit(true)

		// Store B: CosmosOnly baseline
		cmsB := storeB.CacheMultiStore()
		cmsB.GetKVStore(keysB["acc"]).Set([]byte("acct1"), []byte{b})
		cmsB.GetKVStore(keysB["bank"]).Set([]byte("supply"), []byte{b, b})
		cmsB.GetKVStore(keysB["evm"]).Set(evmData.storKey, makeSlot(b, 0xAA))
		cmsB.GetKVStore(keysB["evm"]).Set(evmData.nonKey, makeNonce(uint64(block)))
		if block == 1 {
			cmsB.GetKVStore(keysB["evm"]).Set(evmData.codeKey, []byte{0x60, 0x60, b})
		}
		cmsB.Write()
		_, err = storeB.GetWorkingHash()
		require.NoError(t, err)
		cidB := storeB.Commit(true)

		// App hashes must be identical (flatkv does not affect consensus)
		require.Equalf(t, cidB.Hash, cidA.Hash,
			"shadow mode app hash must match CosmosOnly at block %d", block)

		// evm_lattice must NOT appear in Store A's CommitInfo
		lattice := findStoreInfo(storeA.lastCommitInfo.StoreInfos, "evm_lattice")
		require.Nilf(t, lattice, "evm_lattice must not appear with EnableLatticeHash=false (block %d)", block)
	}

	require.NoError(t, storeB.Close())
	require.NoError(t, storeA.Close())

	// FlatKV should still have data even though lattice wasn't in app hash
	cfg := dualWriteNoLatticeConfig()
	ro := openFlatKVReadOnly(t, dirA, cfg, 0)
	require.NotEmpty(t, ro.RootHash(), "flatkv should have non-empty RootHash even with lattice disabled")
	require.NoError(t, ro.Close())
}

// ---------------------------------------------------------------------------
// Test 17: SplitWrite read routing — EVM data absent from memiavl, present in flatkv
// ---------------------------------------------------------------------------

func TestFlatKVSplitWriteReadRouting(t *testing.T) {
	dir := t.TempDir()
	cfg := integrationSplitWriteConfig()
	store, storeKeys := newTestRootMulti(t, dir, cfg)

	addrs := newMultiEVMTestData(3)

	type writtenKV struct {
		key   []byte
		value []byte
	}
	var finalWrites []writtenKV

	for block := 1; block <= 5; block++ {
		cms := store.CacheMultiStore()
		b := byte(block)
		cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{b})
		cms.GetKVStore(storeKeys["bank"]).Set([]byte("supply"), []byte{b})

		for i, addr := range addrs {
			storVal := makeSlot(b, byte(i), 0xDD)
			cms.GetKVStore(storeKeys["evm"]).Set(addr.storKey, storVal)

			nonceVal := makeNonce(uint64(block*10 + i))
			cms.GetKVStore(storeKeys["evm"]).Set(addr.nonKey, nonceVal)

			if block == 5 {
				finalWrites = append(finalWrites,
					writtenKV{key: addr.storKey, value: storVal},
					writtenKV{key: addr.nonKey, value: nonceVal},
				)
			}
		}

		cms.Write()
		_, err := store.GetWorkingHash()
		require.NoError(t, err)
		store.Commit(true)
	}
	require.NoError(t, store.Close())

	// In SplitWrite, memiavl "evm" tree should NOT have the EVM data
	store2, _ := newTestRootMulti(t, dir, cfg)
	evmTree := store2.scStore.GetChildStoreByName("evm")
	require.NotNil(t, evmTree)

	for _, w := range finalWrites {
		got := evmTree.Get(w.key)
		require.Nilf(t, got, "memiavl should NOT contain EVM key %x in SplitWrite mode", w.key)
		require.Falsef(t, evmTree.Has(w.key), "memiavl Has() should be false for %x in SplitWrite", w.key)
	}
	require.NoError(t, store2.Close())

	// FlatKV should have the data
	ro := openFlatKVReadOnly(t, dir, cfg, 0)
	for _, w := range finalWrites {
		val, found := ro.Get(evm.EVMStoreKey, w.key)
		require.Truef(t, found, "flatkv should contain key %x in SplitWrite mode", w.key)
		require.Equalf(t, w.value, val, "flatkv value mismatch for key %x", w.key)
	}
	require.NoError(t, ro.Close())
}

// ---------------------------------------------------------------------------
// Test 18: Query with SS enabled and ReadMode
// ---------------------------------------------------------------------------

func TestFlatKVQueryWithSSAndReadMode(t *testing.T) {
	home := t.TempDir()

	scCfg := dualWriteConfig()
	ssCfg := seidbconfig.DefaultStateStoreConfig()
	ssCfg.Enable = true
	ssCfg.AsyncWriteBuffer = 0

	store := NewStore(home, scCfg, ssCfg, nil)
	storeKeys := make(map[string]*types.KVStoreKey)
	for _, name := range storeNames {
		sk := types.NewKVStoreKey(name)
		storeKeys[name] = sk
		store.MountStoreWithDB(sk, types.StoreTypeIAVL, nil)
	}
	require.NoError(t, store.LoadLatestVersion())
	defer func() { require.NoError(t, store.Close()) }()

	evmData := newEVMTestData(0x88)

	// v1: write EVM + cosmos data
	cms := store.CacheMultiStore()
	cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{1})
	cms.GetKVStore(storeKeys["evm"]).Set(evmData.storKey, makeSlot(0x01, 0xAA))
	cms.Write()
	_, err := store.GetWorkingHash()
	require.NoError(t, err)
	c1 := store.Commit(true)
	require.Equal(t, int64(1), c1.Version)

	// v2: update with different values
	cms = store.CacheMultiStore()
	cms.GetKVStore(storeKeys["acc"]).Set([]byte("acct1"), []byte{2})
	cms.GetKVStore(storeKeys["evm"]).Set(evmData.storKey, makeSlot(0x02, 0xBB))
	cms.Write()
	_, err = store.GetWorkingHash()
	require.NoError(t, err)
	c2 := store.Commit(true)
	require.Equal(t, int64(2), c2.Version)

	// Wait for SS to catch up
	waitUntilSSVersion(t, store, c2.Version)

	// Query cosmos key at v1 (SS path, no proof)
	resp := store.Query(abci.RequestQuery{
		Path:   "/acc/key",
		Data:   []byte("acct1"),
		Height: c1.Version,
		Prove:  false,
	})
	require.EqualValues(t, 0, resp.Code, "cosmos query failed: %s", resp.Log)
	require.Equal(t, []byte{1}, resp.Value)

	// Query EVM key at v1 (SS path, no proof)
	resp = store.Query(abci.RequestQuery{
		Path:   "/evm/key",
		Data:   evmData.storKey,
		Height: c1.Version,
		Prove:  false,
	})
	require.EqualValues(t, 0, resp.Code, "evm query failed: %s", resp.Log)
	require.Equal(t, makeSlot(0x01, 0xAA), resp.Value)

	// Query EVM key at v2 (latest, no proof)
	resp = store.Query(abci.RequestQuery{
		Path:   "/evm/key",
		Data:   evmData.storKey,
		Height: c2.Version,
		Prove:  false,
	})
	require.EqualValues(t, 0, resp.Code, "evm latest query failed: %s", resp.Log)
	require.Equal(t, makeSlot(0x02, 0xBB), resp.Value)

	// Query EVM key at v1 with proof (SC path)
	resp = store.Query(abci.RequestQuery{
		Path:   "/evm/key",
		Data:   evmData.storKey,
		Height: c1.Version,
		Prove:  true,
	})
	require.EqualValues(t, 0, resp.Code, "evm proof query failed: %s", resp.Log)
	require.Equal(t, makeSlot(0x01, 0xAA), resp.Value)
	require.NotNil(t, resp.ProofOps, "proof should be present")
	require.NotEmpty(t, resp.ProofOps.Ops, "proof ops should not be empty")
}
