package siena.base.test.model;

import siena.Generator;
import siena.Id;
import siena.Table;

@Table("embedded_sub_models")
public class EmbeddedSubModel{
    @Id(Generator.NONE)
    public String id;
    
    public EmbeddedModel parent;           
    
    public String toString() {
    	return "id:"+id + " - parent:" + parent;
    }
}
