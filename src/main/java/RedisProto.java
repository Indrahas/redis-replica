

class RedisDecodedInfo{
    public String Content;
    public Integer Offset;
    public RedisDecodedInfo(String Content, Integer Offset){
        this.Content = Content;
        this.Offset = Offset;
    }
}
public class RedisProto {
    public static String Encode(String[] Request){
        String[] ToReturn = new String[Request.length + 1];
        ToReturn[0] = "*" + Request.length;
        for(Integer I = 1; I <= Request.length; ++I){
            ToReturn[I] = RedisProto.Encode(Request[I - 1]);
        }
        return String.join("\r\n", ToReturn) + "\r\n";
    }
    public static String Encode(String Request){
        return "$" + Request.length() + "\r\n" + Request;
    }
    public static String[] Decode(String Request, int[] endIndex) throws Exception {
        String Type = Request.substring(0, 1);
        Integer Index = Request.indexOf("\r\n");
        Integer Count = Integer.valueOf(Request.substring(1, Index));

        if(Type.equals("*")){
            String[] ToReturn = new String[Count];
            Integer RemovedLength = Index;
            RedisDecodedInfo Info;
            for(Integer I = 0; I < Count; ++I){
                Info = RedisProto.DecodeHelper(Request.substring(RemovedLength + 2));
                ToReturn[I] = Info.Content;
                RemovedLength = Info.Offset + RemovedLength;
            }
            endIndex[0] += RemovedLength+2;
            return ToReturn;
        } else if(Type.equals("$")) {
            return new String[]{RedisProto.DecodeHelper(Request.substring(Count + 2)).Content};
        } else {
            throw new Exception("Invalid Data");
        }
    }
    private static RedisDecodedInfo DecodeHelper(String Request) throws Exception {
        String Type = Request.substring(0, 1);
        Integer Index = Request.indexOf("\r\n") + 2;
        Integer Count = Integer.valueOf(Request.substring(1, Index - 2));
        if(Type.equals("$")){
            // I am a string
            return new RedisDecodedInfo(Request.substring(Index, Index + Count), Index + Count + 2);
        } else if(Type.equals("-")){
            throw new Exception(Request.substring(1, Index));
        } else {
            throw new Exception("Unknown Data Type");
        }
    }
}
