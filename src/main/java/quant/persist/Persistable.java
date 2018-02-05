package quant.persist;

public interface Persistable<T> {
	
	T read();
	
	void write(T t);
	
}
