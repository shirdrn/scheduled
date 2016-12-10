package cn.shiyanjun.platform.scheduled.common;

public class ResourceInsufficientException extends Exception {

	private static final long serialVersionUID = 1L;

	public ResourceInsufficientException() {
        super();
    }
	
	public ResourceInsufficientException(String message) {
        super(message);
    }
	
	public ResourceInsufficientException(String message, Throwable cause) {
        super(message, cause);
    }
}
