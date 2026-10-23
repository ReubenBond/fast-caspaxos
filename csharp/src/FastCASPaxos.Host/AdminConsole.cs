using Microsoft.Extensions.FileProviders;

namespace FastCASPaxos.Host;

public static class AdminConsole
{
    public static void MapAdminConsole(this WebApplication app)
    {
        var root = Path.Combine(AppContext.BaseDirectory, "wwwroot", "admin");
        if (Directory.Exists(root))
        {
            var files = new PhysicalFileProvider(root);
            app.Lifetime.ApplicationStopped.Register(files.Dispose);
            app.UseStaticFiles(new StaticFileOptions
            {
                FileProvider = files,
                RequestPath = "/admin",
                OnPrepareResponse = context =>
                    context.Context.Response.Headers.CacheControl =
                        context.File.Name == "index.html" ? "no-store" : "public,max-age=31536000,immutable",
            });
        }

        app.MapGet("/admin", (HttpContext context) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            if (!context.Request.Path.Value!.EndsWith('/'))
                return Results.Redirect("/admin/");
            var index = Path.Combine(root, "index.html");
            return File.Exists(index)
                ? Results.File(index, "text/html")
                : Results.Problem(statusCode: 503, title: "admin-assets-unavailable",
                    detail: "Build or publish FastCASPaxos.Host with Node.js 24+ and BuildAdminConsole=true to include the administration console.");
        });
    }
}
