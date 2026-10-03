package columnar

import "os"

func writeByteAt(path string, pos int64, b byte) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt([]byte{b}, pos)
	return err
}
