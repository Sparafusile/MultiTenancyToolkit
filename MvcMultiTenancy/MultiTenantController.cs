namespace MvcMultiTenancy
{
    public abstract class MultiTenantController : System.Web.Mvc.Controller
    {
        /// <summary>
        /// The current subdomain in the url.
        /// </summary>
        protected string Subdomain
        {
            get { return Request.Headers["HOST"].Split( '.' )[0]; }
        }

        public abstract string GetTenantName();

        public abstract string GetTenantSchema();
    }
}
