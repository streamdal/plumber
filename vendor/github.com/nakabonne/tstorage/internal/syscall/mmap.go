package syscall

func Mmap(fd, length int) ([]byte, error) {
	return mmap(fd, length)
}
