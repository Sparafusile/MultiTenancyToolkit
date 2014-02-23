using System.IO;
using System.Web.Mvc;
using System.Web.Hosting;
using System.Diagnostics;

namespace MvcMultiTenancy
{
    //protected void Application_Start()
    //{
    //    System.Web.Mvc.ViewEngines.Engines.Clear();
    //    System.Web.Mvc.ViewEngines.Engines.Add( new CS.Common.MulitTenantRazorViewEngine() );

    public class MulitTenantRazorViewEngine : RazorViewEngine
    {
        public static readonly string[] DefaultAreaLayoutFormats =
        {
            "~/Areas/{2}/Views/{1}/{0}.cshtml",
            "~/Areas/{2}/Views/{1}/{0}.vbhtml",
            "~/Areas/{2}/Views/Shared/{0}.cshtml",
            "~/Areas/{2}/Views/Shared/{0}.vbhtml"
        };

        public static readonly string[] DefaultViewLayoutFormats =
        {
            "~/Views/%1/{1}/{0}.cshtml",
            "~/Views/%1/Shared/{0}.cshtml",
            "~/Views/Default/{1}/{0}.cshtml",
            "~/Views/Default/Shared/{0}.cshtml"
        };

        public static string GetLayoutForTenant( string Controller, string Tenant, string NameUrl )
        {
            // Check to see if the given NameUrl is a valid path
            var layoutUrl = string.Format( NameUrl, Controller, Tenant );
            var fullPath = HostingEnvironment.IsHosted ? HostingEnvironment.MapPath( NameUrl ) : Path.GetFullPath( NameUrl );
            if( File.Exists( fullPath ) ) return layoutUrl;

            // Loop through the layout formats and find one that exists
            foreach( var item in MulitTenantRazorViewEngine.DefaultViewLayoutFormats )
            {
                layoutUrl = string.Format( item, NameUrl, Controller, Tenant );
                fullPath = HostingEnvironment.IsHosted ? HostingEnvironment.MapPath( layoutUrl ) : Path.GetFullPath( layoutUrl );
                if( File.Exists( fullPath ) ) return layoutUrl;
            }

            return null;
        }

        public MulitTenantRazorViewEngine()
        {
            AreaViewLocationFormats = MulitTenantRazorViewEngine.DefaultAreaLayoutFormats;
            AreaMasterLocationFormats = MulitTenantRazorViewEngine.DefaultAreaLayoutFormats;
            AreaPartialViewLocationFormats = MulitTenantRazorViewEngine.DefaultAreaLayoutFormats;

            ViewLocationFormats = MulitTenantRazorViewEngine.DefaultViewLayoutFormats;
            MasterLocationFormats = MulitTenantRazorViewEngine.DefaultViewLayoutFormats;
            PartialViewLocationFormats = MulitTenantRazorViewEngine.DefaultViewLayoutFormats;
        }

        protected override IView CreatePartialView( ControllerContext controllerContext, string partialPath )
        {
            var PassedController = controllerContext.Controller as MultiTenantController;
            Debug.Assert( PassedController != null, "PassedController != null" );
            return base.CreatePartialView( controllerContext, partialPath.Replace( "%1", PassedController.GetTenantSchema() ) );
        }

        protected override IView CreateView( ControllerContext controllerContext, string viewPath, string masterPath )
        {
            var PassedController = controllerContext.Controller as MultiTenantController;
            Debug.Assert( PassedController != null, "PassedController != null" );
            var view = base.CreateView
            (
                controllerContext, viewPath.Replace( "%1", PassedController.GetTenantSchema() ),
                masterPath.Replace( "%1", PassedController.GetTenantSchema() )
            );

            return view;
        }

        protected override bool FileExists( ControllerContext controllerContext, string virtualPath )
        {
            var PassedController = controllerContext.Controller as MultiTenantController;
            Debug.Assert( PassedController != null, "PassedController != null" );
            return base.FileExists( controllerContext, virtualPath.Replace( "%1", PassedController.GetTenantSchema() ) );
        }
    }

    //<system.web.webPages.razor>
    //  <host factoryType="System.Web.Mvc.MvcWebRazorHostFactory, System.Web.Mvc, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31BF3856AD364E35" />
    //  <pages pageBaseType="Mv4App.CustomWebViewPage">

    public class MulitTenantWebViewPage : WebViewPage
    {
        public override void Execute()
        {
        }
    }

    public class MulitTenantWebViewPage<T> : WebViewPage<T>
    {
        public override void Execute()
        {
        }
    }
}
