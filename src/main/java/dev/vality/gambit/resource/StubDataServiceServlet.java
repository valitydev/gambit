package dev.vality.gambit.resource;

import dev.vality.gambit.StubDataServiceSrv;
import dev.vality.woody.thrift.impl.http.THServiceBuilder;
import lombok.RequiredArgsConstructor;

import jakarta.servlet.*;
import jakarta.servlet.annotation.WebServlet;
import java.io.IOException;

@WebServlet("/stubdata/v1/")
@RequiredArgsConstructor
public class StubDataServiceServlet extends GenericServlet {

    private final StubDataServiceSrv.Iface stubDataServiceHandler;
    private Servlet thriftServlet;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(StubDataServiceSrv.Iface.class, stubDataServiceHandler);
    }

    @Override
    public void service(ServletRequest request, ServletResponse response) throws ServletException, IOException {
        thriftServlet.service(request, response);
    }

}
