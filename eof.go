package columnar

import "io"

// ioEOF 是扫描结束哨兵，Next 返回的错误可用 errors.Is(err, io.EOF) 判断。
var ioEOF = io.EOF
