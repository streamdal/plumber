package tstorage

type fakeEncoder struct {
	encodePointFunc func(*DataPoint) error
	flushFunc       func() error
}

func (f *fakeEncoder) encodePoint(p *DataPoint) error {
	if f.encodePointFunc == nil {
		return nil
	}
	return f.encodePointFunc(p)
}

func (f *fakeEncoder) flush() error {
	if f.flushFunc == nil {
		return nil
	}
	return f.flushFunc()
}
