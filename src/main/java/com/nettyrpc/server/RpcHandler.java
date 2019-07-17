package com.nettyrpc.server;

import com.nettyrpc.protocol.RpcRequest;
import com.nettyrpc.protocol.RpcResponse;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC Handler（RPC request processor）
 *
 * SimpleChannelInboundHandler 是对ChannelInboundHandlerAdapter的进一层封装，
 * 提供了站的消息可以通过泛型来规定。SimpleChannelInboundHandler使用了适配器模式
 * @author luxiaoxun
 */
public class RpcHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RpcHandler.class);

    private final Map<String, Object> handlerMap;

    public RpcHandler(Map<String, Object> handlerMap) {
        this.handlerMap = handlerMap;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final RpcRequest request) throws Exception {
        RpcServer.submit(new Runnable() {
            @Override
            public void run() {
                logger.debug("Receive request " + request.getRequestId());
                RpcResponse response = new RpcResponse();
                response.setRequestId(request.getRequestId());
                try {
                    Object result = handle(request);
                    response.setResult(result);
                } catch (Throwable t) {
                    response.setError(t.toString());
                    logger.error("RPC Server handle request error", t);
                }
                /**
                 * ChannelFuture 是一个将channel操作返回结果的处理对象，
                 * 异步处理立即返回该对象，生命周期有两种时间段
                 * 1：还未完成。
                 *    isDone（）；isSuccess（）；isCancelled（）都是false cause() null
                 * 2：已完成。
                 *    sucess   isDone（）true；isSuccess（） true
                 *    faliure  isDone（）true；isSuccess（）false  cause() non-null
                 *    cancell  isDone（）true；isCancelled（）true
                 */
                ChannelFuture channelFuture = ctx.writeAndFlush(response);
                /**
                 * 添加异步回调机制， 处理当前channel用的异步结果
                 */
                channelFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                        if(channelFuture.isDone() && channelFuture.isSuccess()){
                            logger.debug("Send response for request " + request.getRequestId());
                        }

                    }
                });
            }
        });
    }

    /**
     * 函数主要是将C端发送的函数信息，在根据自定义的协议解析后获取相应的函数信息，
     * 类名，方法名，方法参数类的数组，方法参数数组
     * 为了节省创建类的时间和消耗的资源，根据类名直接从map中直接获取类已创建的对象
     * 然后使用 cgLid FastClass代替 jdk 的invoke。
     *
     * @param request
     * @return
     * @throws Throwable
     */
    private Object handle(RpcRequest request) throws Throwable {
        String className   = request.getClassName();
        Object serviceBean = handlerMap.get(className);

        Class<?>   serviceClass   = serviceBean.getClass();
        String     methodName     = request.getMethodName();
        Class<?>[] parameterTypes = request.getParameterTypes();
        Object[]   parameters     = request.getParameters();

        logger.debug(serviceClass.getName());
        logger.debug(methodName);
        for (int i = 0; i < parameterTypes.length; ++i) {
            logger.debug(parameterTypes[i].getName());
        }
        for (int i = 0; i < parameters.length; ++i) {
            logger.debug(parameters[i].toString());
        }

        // JDK reflect
        /*Method method = serviceClass.getMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(serviceBean, parameters);*/

        // Cglib reflect
        FastClass serviceFastClass = FastClass.create(serviceClass);
//        FastMethod serviceFastMethod = serviceFastClass.getMethod(methodName, parameterTypes);
//        return serviceFastMethod.invoke(serviceBean, parameters);
        // for higher-performance
        int methodIndex = serviceFastClass.getIndex(methodName, parameterTypes);
        return serviceFastClass.invoke(methodIndex, serviceBean, parameters);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("server caught exception", cause);
        ctx.close();
    }
}
