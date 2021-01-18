package formats;

import java.io.Serializable;

/* Version sérializable de KV */
public class KVS implements Serializable{

	public static final String SEPARATOR = "<->";
	
	public String k;
	public String v;
	
	public KVS() {}
	
	public KVS(String k, String v) {
		super();
		this.k = k;
		this.v = v;
	}

	public String toString() {
		return "KV [k=" + k + ", v=" + v + "]";
	}
	
}
