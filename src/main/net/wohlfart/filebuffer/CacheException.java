package net.wohlfart.filebuffer;

public class CacheException extends RuntimeException {
	private static final long serialVersionUID = 1;

	public CacheException(String message, Throwable throwable) {
		super(message, throwable);
	}

	public CacheException(String message) {
		super(message);
	}
}
